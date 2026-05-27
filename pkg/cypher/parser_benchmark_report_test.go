package cypher

import (
	"fmt"
	"os"
	"sort"
	"strings"
	"testing"
	"time"

	antlrparser "github.com/orneryd/nornicdb/pkg/cypher/antlr"
)

type parserBenchmarkResult struct {
	name   string
	time   time.Duration
	status string
}

func measureMedianParserDuration(samples int, run func() error) (time.Duration, error) {
	if samples < 1 {
		samples = 1
	}

	durations := make([]time.Duration, 0, samples)
	for i := 0; i < samples; i++ {
		start := time.Now()
		if err := run(); err != nil {
			return time.Since(start), err
		}
		durations = append(durations, time.Since(start))
	}

	sort.Slice(durations, func(i, j int) bool {
		return durations[i] < durations[j]
	})
	return durations[len(durations)/2], nil
}

func printParserReportComparison(title, mode string, nornicResults, antlrResults []parserBenchmarkResult, totalNornic, totalANTLR time.Duration, successNornic, successANTLR int) {
	fmt.Println("\n" + strings.Repeat("=", 96))
	fmt.Println(title)
	fmt.Println(strings.Repeat("=", 96))
	fmt.Printf("\n%-30s | %-12s | %-12s | %-8s | %s\n", "Query", "Nornic", "ANTLR", "Ratio", "Status")
	fmt.Println(strings.Repeat("-", 96))

	for i := range nornicResults {
		nr := nornicResults[i]
		ar := antlrResults[i]

		status := "ok"
		if nr.status != "ok" || ar.status != "ok" {
			switch {
			case nr.status != "ok" && ar.status != "ok":
				status = "both failed"
			case nr.status != "ok":
				status = "nornic failed"
			default:
				status = "antlr failed"
			}
		}

		ratio := 0.0
		if nr.time > 0 {
			ratio = float64(ar.time) / float64(nr.time)
		}

		name := nr.name
		if len(name) > 30 {
			name = name[:27] + "..."
		}
		fmt.Printf("%-30s | %12s | %12s | %7.1fx | %s\n", name, nr.time, ar.time, ratio, status)
	}

	fmt.Println(strings.Repeat("-", 96))
	avgRatio := 0.0
	if totalNornic > 0 {
		avgRatio = float64(totalANTLR) / float64(totalNornic)
	}
	fmt.Printf("%-30s | %12s | %12s | %7.1fx |\n", "TOTAL", totalNornic, totalANTLR, avgRatio)
	fmt.Println(strings.Repeat("=", 96))
	fmt.Printf("\nSummary: Nornic %.1fx faster than ANTLR for %s on this corpus\n", avgRatio, mode)
	fmt.Printf("PARSER_REPORT_SUMMARY mode=%s parser=Nornic total_ns=%d success=%d total_cases=%d\n", mode, totalNornic.Nanoseconds(), successNornic, len(nornicResults))
	fmt.Printf("PARSER_REPORT_SUMMARY mode=%s parser=ANTLR total_ns=%d success=%d total_cases=%d\n\n", mode, totalANTLR.Nanoseconds(), successANTLR, len(antlrResults))
}

// TestParserValidationBenchmarkReport prints a machine-readable validation-only parser report.
// Run explicitly with: NORNIC_RUN_PARSER_REPORTS=1 go test -run TestParserValidationBenchmarkReport ./pkg/cypher -count=1 -v
func TestParserValidationBenchmarkReport(t *testing.T) {
	if os.Getenv("NORNIC_RUN_PARSER_REPORTS") == "" {
		t.Skip("benchmark-style parser report; set NORNIC_RUN_PARSER_REPORTS=1 to run")
	}

	const samplesPerQuery = 5
	exec := &StorageExecutor{}
	nornicResults := make([]parserBenchmarkResult, 0, len(testQueries))
	antlrResults := make([]parserBenchmarkResult, 0, len(testQueries))
	var totalNornic, totalANTLR time.Duration
	var successNornic, successANTLR int

	for _, tc := range testQueries {
		nornicTime, nornicErr := measureMedianParserDuration(samplesPerQuery, func() error {
			return exec.validateSyntaxNornic(tc.query)
		})
		nornicStatus := "ok"
		if nornicErr != nil {
			nornicStatus = "failed: " + nornicErr.Error()
		} else {
			totalNornic += nornicTime
			successNornic++
		}
		nornicResults = append(nornicResults, parserBenchmarkResult{name: tc.name, time: nornicTime, status: nornicStatus})

		antlrTime, antlrErr := measureMedianParserDuration(samplesPerQuery, func() error {
			return antlrparser.Validate(tc.query)
		})
		antlrStatus := "ok"
		if antlrErr != nil {
			antlrStatus = "failed: " + antlrErr.Error()
		} else {
			totalANTLR += antlrTime
			successANTLR++
		}
		antlrResults = append(antlrResults, parserBenchmarkResult{name: tc.name, time: antlrTime, status: antlrStatus})
	}

	printParserReportComparison("CYPHER VALIDATION BENCHMARK REPORT", "validate", nornicResults, antlrResults, totalNornic, totalANTLR, successNornic, successANTLR)

	if successNornic == 0 || successANTLR == 0 {
		t.Fatalf("expected both parsers to validate at least one query (nornic=%d antlr=%d)", successNornic, successANTLR)
	}
}

// TestParserParseBenchmarkReport prints a machine-readable full parse report.
// The Nornic side uses ASTBuilder.Build(), which is the structured AST-building
// path, rather than the older skeletal Parser.Parse() surface.
// Run explicitly with: NORNIC_RUN_PARSER_REPORTS=1 go test -run TestParserParseBenchmarkReport ./pkg/cypher -count=1 -v
func TestParserParseBenchmarkReport(t *testing.T) {
	if os.Getenv("NORNIC_RUN_PARSER_REPORTS") == "" {
		t.Skip("benchmark-style parser report; set NORNIC_RUN_PARSER_REPORTS=1 to run")
	}

	const samplesPerQuery = 5
	astBuilder := NewASTBuilder()
	nornicResults := make([]parserBenchmarkResult, 0, len(testQueries))
	antlrResults := make([]parserBenchmarkResult, 0, len(testQueries))
	var totalNornic, totalANTLR time.Duration
	var successNornic, successANTLR int

	for _, tc := range testQueries {
		nornicTime, nornicErr := measureMedianParserDuration(samplesPerQuery, func() error {
			_, err := astBuilder.Build(tc.query)
			return err
		})
		nornicStatus := "ok"
		if nornicErr != nil {
			nornicStatus = "failed: " + nornicErr.Error()
		} else {
			totalNornic += nornicTime
			successNornic++
		}
		nornicResults = append(nornicResults, parserBenchmarkResult{name: tc.name, time: nornicTime, status: nornicStatus})

		antlrTime, antlrErr := measureMedianParserDuration(samplesPerQuery, func() error {
			_, err := antlrparser.Parse(tc.query)
			return err
		})
		antlrStatus := "ok"
		if antlrErr != nil {
			antlrStatus = "failed: " + antlrErr.Error()
		} else {
			totalANTLR += antlrTime
			successANTLR++
		}
		antlrResults = append(antlrResults, parserBenchmarkResult{name: tc.name, time: antlrTime, status: antlrStatus})
	}

	printParserReportComparison("CYPHER PARSE BENCHMARK REPORT", "parse", nornicResults, antlrResults, totalNornic, totalANTLR, successNornic, successANTLR)

	if successNornic == 0 || successANTLR == 0 {
		t.Fatalf("expected both parsers to parse at least one query (nornic=%d antlr=%d)", successNornic, successANTLR)
	}
}
