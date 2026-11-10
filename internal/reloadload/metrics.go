package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
)

const reloadFamily = "microgateway_config_reloads_total"

// parseLabels reads a label set that starts with '{' and returns its labels and the text after the closing '}'.
// Label values may contain the escapes \\, \" and \n, and commas and braces inside the quotes.
func parseLabels(s string) (map[string]string, string, error) {
	labels := map[string]string{}
	i := 1
	for {
		if i < len(s) && s[i] == '}' {
			return labels, s[i+1:], nil
		}
		eq := strings.IndexByte(s[i:], '=')
		if eq <= 0 {
			return nil, "", errors.New("label without a name and a value")
		}
		key := s[i : i+eq]
		i += eq + 1
		if i >= len(s) || s[i] != '"' {
			return nil, "", fmt.Errorf("label %s value is not quoted", key)
		}
		i++
		var val strings.Builder
		for {
			if i >= len(s) {
				return nil, "", fmt.Errorf("label %s value is not terminated", key)
			}
			c := s[i]
			i++
			if c == '"' {
				break
			}
			if c != '\\' {
				val.WriteByte(c)
				continue
			}
			if i >= len(s) {
				return nil, "", fmt.Errorf("label %s value is not terminated", key)
			}
			switch s[i] {
			case '\\', '"':
				val.WriteByte(s[i])
			case 'n':
				val.WriteByte('\n')
			default:
				return nil, "", fmt.Errorf("label %s has an invalid escape", key)
			}
			i++
		}
		labels[key] = val.String()
		if i < len(s) && s[i] == ',' {
			i++
			continue
		}
		if i < len(s) && s[i] == '}' {
			return labels, s[i+1:], nil
		}
		return nil, "", errors.New("labels are not closed")
	}
}

// parseSample reads the labels and the value of one sample line that follows its metric name.
func parseSample(s string) (map[string]string, float64, error) {
	labels := map[string]string{}
	if strings.HasPrefix(s, "{") {
		var err error
		labels, s, err = parseLabels(s)
		if err != nil {
			return nil, 0, err
		}
	}
	fields := strings.Fields(s)
	if len(fields) == 0 {
		return nil, 0, errors.New("missing value")
	}
	v, err := strconv.ParseFloat(fields[0], 64)
	if err != nil {
		return nil, 0, err
	}
	return labels, v, nil
}

// reloadCounts reads the success, failure and unchanged series of the reload counter family from Prometheus text.
func reloadCounts(text []byte) (success, failure, unchanged float64, err error) {
	seen := map[string]float64{}
	for _, line := range strings.Split(string(text), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		name, rest := line, ""
		if i := strings.IndexAny(line, "{ "); i >= 0 {
			name, rest = line[:i], line[i:]
		}
		if name != reloadFamily {
			continue
		}
		labels, value, perr := parseSample(rest)
		if perr != nil {
			return 0, 0, 0, fmt.Errorf("%s: malformed line %q: %w", reloadFamily, line, perr)
		}
		if result, ok := labels["result"]; ok {
			seen[result] = value
		}
	}
	get := func(result string) (float64, error) {
		v, ok := seen[result]
		if !ok {
			return 0, fmt.Errorf("%s{result=%q} not found", reloadFamily, result)
		}
		return v, nil
	}
	if success, err = get("success"); err != nil {
		return 0, 0, 0, err
	}
	if failure, err = get("failure"); err != nil {
		return 0, 0, 0, err
	}
	if unchanged, err = get("unchanged"); err != nil {
		return 0, 0, 0, err
	}
	return success, failure, unchanged, nil
}

// scrape reads the reload counters from the admin endpoint of one gateway.
func scrape(ctx context.Context, c *http.Client, admin string) (Counts, error) {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+admin+"/metrics", nil)
	if err != nil {
		return Counts{}, err
	}
	resp, err := c.Do(req)
	if err != nil {
		return Counts{}, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return Counts{}, fmt.Errorf("GET /metrics: status %d", resp.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if err != nil {
		return Counts{}, err
	}
	s, f, u, err := reloadCounts(body)
	if err != nil {
		return Counts{}, err
	}
	return Counts{Success: s, Failure: f, Unchanged: u}, nil
}
