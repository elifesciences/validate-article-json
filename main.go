package main

// "If you want a fast and correct validator, pick santhosh-tekuri/jsonschema."
// - https://dev.to/vearutop/benchmarking-correctness-and-performance-of-go-json-schema-validators-3247

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/santhosh-tekuri/jsonschema/v5"
	"github.com/tidwall/gjson"
)

// todo:
// https://json-schema.org/understanding-json-schema/reference/regular_expressions.html
// https://github.com/santhosh-tekuri/jsonschema/issues/113

func panic_on_err(err error, action string) {
	if err != nil {
		panic(fmt.Sprintf("failed with '%s' while '%s'", err.Error(), action))
	}
}

func slurp_bytes(path string) []byte {
	body, err := os.ReadFile(path)
	panic_on_err(err, "slurping bytes from path: "+path)
	return body
}

type Foo struct {
	Label  string
	Path   string
	Schema *jsonschema.Schema
}

func configure_validator() map[string]Foo {
	loader := jsonschema.Loaders["file"]
	c := jsonschema.NewCompiler()
	c.Draft = jsonschema.Draft4
	schema_file_list := map[string]string{
		"POA": "api-raml/dist/model/article-poa.v3.json",
		"VOR": "api-raml/dist/model/article-vor.v7.json",
	}
	schema_map := map[string]Foo{}
	for label, path := range schema_file_list {
		rdr, err := loader(path)
		panic_on_err(err, fmt.Sprintf("loading '%s' schema file: ", label, path))
		err = c.AddResource(label, rdr)
		panic_on_err(err, "adding schema to compiler: "+label)
		schema, err := c.Compile(label)
		panic_on_err(err, "compiling schema: "+label)
		schema_map[label] = Foo{
			Label:  label,
			Path:   path,
			Schema: schema,
		}
	}
	return schema_map
}

func read_article_data(article_json_path string) (string, interface{}) {
	article_json_bytes := slurp_bytes(article_json_path)
	result := gjson.GetBytes(article_json_bytes, "article.status")
	if !result.Exists() {
		panic("'article.status' field in article data not found: " + article_json_path)
	}
	schema_key := strings.ToUpper(result.String()) // "poa" => "POA"

	// article-json contains 'journal', 'snippet' and 'article' sections.
	// extract just the 'article' from the article data.
	result = gjson.GetBytes(article_json_bytes, "article")
	if !result.Exists() {
		panic("'article' field in article data not found: " + article_json_path)
	}

	// what is happening here?? the slice of matching bytes are extracted from
	// the article-json, skipping a conversion of `result` to a string then back
	// to bytes for unmarshalling. if only a `result.Bytes()` existed :(
	// - https://github.com/tidwall/gjson#user-content-working-with-bytes
	var raw []byte
	if result.Index > 0 {
		raw = article_json_bytes[result.Index : result.Index+len(result.Raw)]
	} else {
		raw = []byte(result.Raw)
	}

	// convert the article-json data into a simple go datatype
	var article interface{}
	err := json.Unmarshal(raw, &article)
	panic_on_err(err, "unmarshalling article section bytes")

	return schema_key, article
}

func validate(schema Foo, article interface{}) (bool, time.Duration) {
	start := time.Now()
	err := schema.Schema.Validate(article)
	t := time.Now()
	elapsed := t.Sub(start)
	if err != nil {
		return false, elapsed
	}
	return true, elapsed

}

func main() {
	args := os.Args[1:]
	article_json_path := args[0]
	schema_map := configure_validator()

	// read article data and determine schema to use
	schema_key, article := read_article_data(article_json_path)
	schema, present := schema_map[schema_key]
	if !present {
		panic("schema not found: " + schema_key)
	}

	// validate!
	success, elapsed := validate(schema, article)

	// "VOR valid after 2.689794ms", "POA invalid after 123.4ms"
	msg := "%s %s after %s"
	if success {
		println(fmt.Sprintf(msg, schema.Label, "valid", elapsed))
		os.Exit(0)
	} else {
		println(fmt.Sprintf(msg, schema.Label, "invalid", elapsed))
		os.Exit(1)
	}
}
