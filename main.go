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

func fail(msg string) int {
	println(msg)
	return 1
}

func success(msg string) int {
	println(msg)
	return 0
}

type Foo struct {
	Label  string
	Path   string
	Schema *jsonschema.Schema
}

func main() {

	// parse args

	args := os.Args[1:]
	article_json_path := args[0]

	// configure validator

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

	// read article data and determine schema to use

	article_json_bytes := slurp_bytes(article_json_path)
	result := gjson.GetBytes(article_json_bytes, "article.status")
	if !result.Exists() {
		panic("'article.status' field in article data not found: " + article_json_path)
	}
	schema_key := strings.ToUpper(result.String()) // "poa" => "POA"
	schema, present := schema_map[schema_key]
	if !present {
		panic("schema not found: " + schema_key)
	}

	// article-json contains 'journal', 'snippet' and 'article' sections.
	// extract just the 'article' from the article data.

	result = gjson.GetBytes(article_json_bytes, "article")
	if !result.Exists() {
		panic("'article' field in article data not found: " + article_json_path)
	}
	// what is happening here?? we're extracting a slice of the matching bytes
	// from the article-json without converting it to a string then back to bytes.
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

	// finally, validate!

	start := time.Now()
	err = schema.Schema.Validate(article)
	if err != nil {
		os.Exit(fail("input file is not valid: " + err.Error()))
	}
	t := time.Now()
	elapsed := t.Sub(start)
	os.Exit(success(fmt.Sprintf("%s article validated in %s", schema.Label, elapsed)))
}
