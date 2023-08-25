package main

// "If you want a fast and correct validator, pick santhosh-tekuri/jsonschema."
// - https://dev.to/vearutop/benchmarking-correctness-and-performance-of-go-json-schema-validators-3247

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/sourcegraph/conc/pool"

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

type Foo struct {
	Label  string
	Path   string
	Schema *jsonschema.Schema
}

var SCHEMA_MAP map[string]Foo

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

func init() {
	SCHEMA_MAP = configure_validator()
}

// ---

func read_article_data(article_json_path string) (string, interface{}) {
	article_json_bytes, err := os.ReadFile(article_json_path)
	panic_on_err(err, "reading bytes from path: "+article_json_path)

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
	err = json.Unmarshal(raw, &article)
	panic_on_err(err, "unmarshalling article section bytes")

	return schema_key, article
}

func validate(schema Foo, article interface{}) (bool, time.Duration) {
	start := time.Now()
	err := schema.Schema.Validate(article)
	end := time.Now()
	elapsed := end.Sub(start)
	if err != nil {
		return false, elapsed
	}
	return true, elapsed

}

func path_exists(path string) bool {
	_, err := os.Stat(path)
	return !errors.Is(err, os.ErrNotExist)
}

func path_is_dir(path string) bool {
	fi, err := os.Lstat(path)
	panic_on_err(err, "reading path: "+path)
	return fi.Mode().IsDir()
}

type Result struct {
	Path    string
	Elapsed int64
	Success bool
}

func validate_article(article_json_path string) Result {

	// read article data and determine schema to use
	schema_key, article := read_article_data(article_json_path)
	schema, present := SCHEMA_MAP[schema_key]
	if !present {
		panic("schema not found: " + schema_key)
	}

	// validate!
	success, elapsed := validate(schema, article)

	// "VOR valid after 2.689794ms: elife-09560-v1.xml.json", "POA invalid after 123.4ms: elife-09560-v1.xml.json"
	msg := "%s %s in %4dms: %s"
	fname := filepath.Base(article_json_path)
	elapsed_ms := elapsed.Milliseconds()
	if success {
		println(fmt.Sprintf(msg, schema.Label, "valid", elapsed_ms, fname))
	} else {
		println(fmt.Sprintf(msg, schema.Label, "invalid", elapsed_ms, fname))
	}
	return Result{
		Path:    article_json_path,
		Elapsed: elapsed_ms,
		Success: success,
	}
}

func format_ms(ms int64) string {
	elapsed_str := fmt.Sprintf("%dms", ms)
	if ms > 1000 {
		// seconds
		elapsed_str = fmt.Sprintf("%ds", ms/1000)
	} else if ms > 60000 {
		// minutes
		elapsed_str = fmt.Sprintf("%dm", (ms/1000)/60)
	}
	return elapsed_str
}

func main() {
	args := os.Args[1:]
	// required first argument is path to an xml document or a directory of xml.
	input_path := args[0]

	if !path_exists(input_path) {
		panic("input path does not exist")
	}
	if path_is_dir(input_path) {
		// validate many
		path_list, err := os.ReadDir(input_path)
		panic_on_err(err, "reading contents of directory: "+input_path)
		sample_size := 1000
		if len(args) == 2 {
			// optional second argument is sample size
			sample_size, err = strconv.Atoi(args[1])
			panic_on_err(err, "converting sample size to an integer")
			if sample_size == -1 {
				sample_size = len(path_list)
			}
		}

		// filter directories from path listing
		file_list := []string{}
		for _, path := range path_list[:sample_size] {
			if !path.IsDir() {
				file_list = append(file_list, input_path+path.Name())
			}
		}

		num_workers := 10 // todo: set to num cpus
		p := pool.NewWithResults[Result]().WithMaxGoroutines(num_workers)

		start_time := time.Now()
		for _, file := range file_list {
			file := file
			p.Go(func() Result {
				return validate_article(file)
			})
		}
		result_list := p.Wait()
		end_time := time.Now()
		wall_time_ms := end_time.Sub(start_time).Milliseconds()

		var cpu_time_ms int64
		for _, result := range result_list {
			cpu_time_ms = cpu_time_ms + result.Elapsed
		}

		failures := []Result{}
		for _, result := range result_list {
			if !result.Success {
				failures = append(failures, result)
			}
		}

		println("")
		println(fmt.Sprintf("articles:%d, failures:%d, workers:%d, wall-time:%s, cpu-time:%s, average:%dms", sample_size, len(failures), num_workers, format_ms(wall_time_ms), format_ms(cpu_time_ms), (cpu_time_ms / int64(sample_size))))

		if len(failures) > 0 {
			os.Exit(1)
		}

	} else {
		// assume file or a link pointing to a file, validate single
		result := validate_article(input_path)
		if !result.Success {
			os.Exit(1)
		}
	}
}
