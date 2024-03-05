package main

// "If you want a fast and correct validator, pick santhosh-tekuri/jsonschema."
// - https://dev.to/vearutop/benchmarking-correctness-and-performance-of-go-json-schema-validators-3247

import (
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/santhosh-tekuri/jsonschema/v5"
	"github.com/sourcegraph/conc/pool"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

func panic_on_err(err error, action string) {
	if err != nil {
		panic(fmt.Sprintf("failed with '%s' while '%s'", err.Error(), action))
	}
}

type Schema struct {
	Label  string
	Path   string
	Schema *jsonschema.Schema
}

type Result struct {
	Type     string
	FileName string
	Elapsed  int64
	Success  bool
	// these can get large. I recommend not accumulating them for large jobs with many problems.
	Error error
}

// "VOR valid in      2.6ms: elife-09560-v1.xml.json"
// "POA invalid in  123.4ms: elife-09560-v1.xml.json"
func (r Result) String() string {
	msg := "%s %s in\t%4dms: %s"
	if r.Success {
		return fmt.Sprintf(msg, r.Type, "valid", r.Elapsed, r.FileName)
	}
	return fmt.Sprintf(msg, r.Type, "invalid", r.Elapsed, r.FileName)
}

type Article struct {
	Type     string // POA or VOR
	FileName string
	Data     interface{} // unmarshalled json data
}

func configure_validator(schema_root string) map[string]Schema {
	compiler := jsonschema.NewCompiler()
	compiler.Draft = jsonschema.Draft4
	schema_file_list := map[string]string{
		"POA": path.Join(schema_root, "/dist/model/article-poa.v3.json"),
		"VOR": path.Join(schema_root, "/dist/model/article-vor.v7.json"),
	}

	schema_map := map[string]Schema{}
	for label, path := range schema_file_list {
		file_bytes, err := os.ReadFile(path)
		panic_on_err(err, fmt.Sprintf("reading '%s' schema file: %s", label, path))
		if label == "VOR" {
			// patch ISBN regex as it can't be compiled in Go.
			// todo: this needs a fix upstream in api-raml.
			// - https://json-schema.org/understanding-json-schema/reference/regular_expressions.html
			// - https://github.com/santhosh-tekuri/jsonschema/issues/113
			// - https://github.com/elifesciences/api-raml/blob/8e2ffb573b2c3d2e173c38cd8b9625cf2d5740ad/src/misc/isbn.v1.yaml#L6
			find := "allOf.2.properties.references.items.definitions.book.properties.isbn.pattern"
			replace := "^.+$"
			file_bytes, err = sjson.SetBytes(file_bytes, find, replace)
			panic_on_err(err, fmt.Sprintf("patching ISBN in '%s' schema: %s", label, path))
		}

		err = compiler.AddResource(label, bytes.NewReader(file_bytes))
		panic_on_err(err, "adding schema to compiler: "+label)
		schema, err := compiler.Compile(label)
		panic_on_err(err, "compiling schema: "+label)
		schema_map[label] = Schema{
			Label:  label,
			Path:   path,
			Schema: schema,
		}
	}
	return schema_map
}

// ---

func read_article_data(article_json_path string) Article {
	article_json_bytes, err := os.ReadFile(article_json_path)
	panic_on_err(err, "reading bytes from path: "+article_json_path)

	article_status := gjson.GetBytes(article_json_bytes, "article.status") // "poa", "vor"
	if !article_status.Exists() {
		panic("'article.status' field in article data not found: " + article_json_path)
	}
	schema_key := strings.ToUpper(article_status.String()) // "poa" => "POA"

	// article-json contains 'journal', 'snippet' and 'article' sections.
	// extract just the 'article' from the article data.
	result := gjson.GetBytes(article_json_bytes, "article")
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

	return Article{
		FileName: article_json_path,
		Data:     article,
		Type:     schema_key,
	}
}

func validate(schema Schema, article interface{}) (time.Duration, error) {
	start := time.Now()
	err := schema.Schema.Validate(article)
	end := time.Now()
	elapsed := end.Sub(start)
	return elapsed, err
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

func validate_article(schema_map map[string]Schema, article Article, capture_error bool) Result {
	// read article data and determine schema to use
	schema, present := schema_map[article.Type]
	if !present {
		panic("schema not found: " + article.Type)
	}

	// validate!
	elapsed, err := validate(schema, article.Data)

	r := Result{
		Type:     article.Type, // POA or VOR
		FileName: article.FileName,
		Elapsed:  elapsed.Milliseconds(),
		Success:  err == nil,
	}

	if capture_error && err != nil {
		r.Error = err
	}

	return r
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

func short_validation_error(err error) {
	fmt.Printf("%v\n", err)
}

func long_validation_error(err error) {
	fmt.Printf("%#v\n", err)
}

func die(b bool, msg string) {
	if b {
		fmt.Println(msg)
		os.Exit(1)
	}
}

// keep a buffer of N files in memory at once to feed a pool of validators.
// ensures disk I/O is not a factor in keeping the CPU busy.
func process_files_with_feeder(buffer_size int, num_workers int, file_list []string, schema_map map[string]Schema, capture_errors bool) (time.Time, time.Time, []Result) {
	// read files from disk into buffer

	job_size := len(file_list)
	if job_size < buffer_size {
		buffer_size = job_size
	}
	article_chan := make(chan Article, buffer_size)
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func(article_chan chan Article, wg *sync.WaitGroup) {
		defer wg.Done()
		for _, file := range file_list {
			article_chan <- read_article_data(file)
		}
		close(article_chan)
		//println("(done reading files)")
	}(article_chan, &wg)

	// process articles from `article_chan` until it's closed.

	worker_pool := pool.NewWithResults[Result]()
	if num_workers >= 1 {
		worker_pool = worker_pool.WithMaxGoroutines(num_workers)
	}
	start_time := time.Now()
	for article := range article_chan {
		article := article
		worker_pool.Go(func() Result {
			result := validate_article(schema_map, article, capture_errors)
			println(result.String())
			return result
		})
	}

	wg.Wait()
	result_list := worker_pool.Wait()
	end_time := time.Now()
	return start_time, end_time, result_list
}

func do() {
	schema_root_ptr := flag.String("schema-root", "", "path to api-raml schema root")
	input_path_ptr := flag.String("article-json", "", "path to a article-json file or directory")
	sample_size_ptr := flag.Int("sample-size", -1, "number of article-json files to parse")
	num_workers_ptr := flag.Int("num-workers", 0, "number of workers (goroutines) to process the article-json files\n0 for number of cpu cores, -1 for unbounded")
	// 1k articles is about ~1.5GiB of RAM
	buffer_size_ptr := flag.Int("buffer-size", 2000, "the maximum number of article-json files to keep in memory at once")
	flag.Parse()

	schema_root := *schema_root_ptr
	die(schema_root == "", "--schema-root is required")
	die(!path_exists(schema_root), "--schema-root path does not exist. it should be a path to the api-raml.")
	schema_map := configure_validator(schema_root)

	input_path := *input_path_ptr
	die(input_path == "", "--article-json is required")
	die(!path_exists(input_path), "--article-json path does not exist. it should be a path to an article-json file or a directory of article-json files.")

	sample_size := *sample_size_ptr
	die(sample_size < -1 || sample_size == 0, "--sample-size must be -1 or a value greater than 0")

	num_workers := *num_workers_ptr
	die(num_workers < -1, "--num-workers must be -1 or greater")
	if num_workers == 0 {
		num_workers = runtime.NumCPU()
	}

	buffer_size := *buffer_size_ptr
	die(buffer_size < 1, "--buffer-size must be a positive integer")

	if !path_is_dir(input_path) {
		// validate single
		capture_errors := true
		article := read_article_data(input_path)
		result := validate_article(schema_map, article, capture_errors)
		if !result.Success {
			long_validation_error(result.Error)
			os.Exit(1)
		}
	} else {
		// validate many
		path_list, err := os.ReadDir(input_path)
		panic_on_err(err, "reading contents of directory: "+input_path)

		if sample_size == -1 || sample_size > len(path_list) {
			// validate all files in dir
			sample_size = len(path_list)
		}

		// sort files by filename, numerically, lowest to highest (asc).
		// order of file listings is never guaranteed so sort before we take a sample.
		// note! filename output happens in parallel so progress may *appear* unordered.
		sort.Slice(path_list, func(a, b int) bool {
			return path_list[a].Name() < path_list[b].Name()
		})

		// remove any directories
		file_list := []string{}
		for i := 0; i < sample_size; i++ {
			path := path_list[i]
			if path.IsDir() {
				continue
			}
			file_list = append(file_list, filepath.Join(input_path, path.Name()))
		}

		// reverse the sample (desc) so we do a natural 'count down' to the lowest article.
		slices.Reverse(file_list)

		// ensure the correct sample size is reported after filtering out directories.
		sample_size = len(file_list)

		capture_errors := false
		start_time, end_time, result_list := process_files_with_feeder(buffer_size, num_workers, file_list, schema_map, capture_errors)
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
			println("")
			for _, result := range failures {
				println(result.String())
				if capture_errors {
					short_validation_error(result.Error)
					println("---")
				}
			}
			os.Exit(1)
		}
	}
}

func do_with_profiling(output_filename string) {
	f, err := os.Create(output_filename)
	die(err != nil, "could not create CPU profile")
	defer f.Close()

	err = pprof.StartCPUProfile(f)
	die(err != nil, "could not start CPU profile")

	defer pprof.StopCPUProfile()

	do()
}

func main() {
	profile := os.Getenv("VAJ_PROFILE")
	if profile != "" {
		println("profiling is on")
		println("---")
		do_with_profiling("cpu.prof")
		println("---")
		println("wrote cpu.prof")
	} else {
		do()
	}
}
