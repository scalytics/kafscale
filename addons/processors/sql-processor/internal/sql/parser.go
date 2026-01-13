// Copyright 2025, 2026 Alexander Alten (novatechflow), NovaTechflow (novatechflow.com).
// This project is supported and financed by Scalytics, Inc. (www.scalytics.io).
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package sql

import (
	"fmt"
	"strconv"
	"strings"
)

func Parse(query string) (Query, error) {
	trimmed := strings.TrimSpace(query)
	if trimmed == "" {
		return Query{}, fmt.Errorf("empty query")
	}

	lower := strings.ToLower(trimmed)
	lower = strings.TrimSuffix(lower, ";")
	fields := strings.Fields(lower)
	if len(fields) == 0 {
		return Query{}, fmt.Errorf("empty query")
	}

	switch fields[0] {
	case "show":
		return parseShow(fields)
	case "describe":
		return parseDescribe(fields)
	case "select":
		return parseSelect(fields)
	default:
		return Query{Type: QueryUnknown}, fmt.Errorf("unsupported statement")
	}
}

func parseShow(fields []string) (Query, error) {
	if len(fields) >= 2 && fields[1] == "topics" {
		return Query{Type: QueryShowTopics}, nil
	}
	if len(fields) >= 4 && fields[1] == "partitions" && fields[2] == "from" {
		return Query{Type: QueryShowPartitions, Topic: fields[3]}, nil
	}
	return Query{Type: QueryUnknown}, fmt.Errorf("unsupported show statement")
}

func parseDescribe(fields []string) (Query, error) {
	if len(fields) < 2 {
		return Query{Type: QueryUnknown}, fmt.Errorf("describe requires topic")
	}
	return Query{Type: QueryDescribe, Topic: fields[1]}, nil
}

func parseSelect(fields []string) (Query, error) {
	columns := parseSelectColumns(fields)
	topic := tokenAfter(fields, "from")
	if topic == "" {
		return Query{Type: QueryUnknown}, fmt.Errorf("select requires from <topic>")
	}

	joinType, joinTopic := parseJoin(fields)
	if joinType != "" && joinTopic == "" {
		return Query{Type: QueryUnknown}, fmt.Errorf("join requires topic")
	}

	partition, offsetMin, offsetMax, err := parseFilters(fields)
	if err != nil {
		return Query{Type: QueryUnknown}, err
	}

	return Query{
		Type:       QuerySelect,
		Topic:      topic,
		JoinTopic:  joinTopic,
		JoinType:   joinType,
		Columns:    columns,
		Limit:      tokenAfter(fields, "limit"),
		Partition:  partition,
		OffsetMin:  offsetMin,
		OffsetMax:  offsetMax,
		TimeWindow: tokenAfter(fields, "within"),
		Last:       tokenAfter(fields, "last"),
		Tail:       tokenAfter(fields, "tail"),
		ScanFull:   hasToken(fields, "scan") && hasToken(fields, "full"),
	}, nil
}

func parseJoin(fields []string) (string, string) {
	for i, field := range fields {
		if field == "join" && i+1 < len(fields) {
			return "inner", fields[i+1]
		}
		if field == "left" && i+2 < len(fields) && fields[i+1] == "join" {
			return "left", fields[i+2]
		}
	}
	return "", ""
}

func tokenAfter(fields []string, token string) string {
	for i, field := range fields {
		if field == token && i+1 < len(fields) {
			return fields[i+1]
		}
	}
	return ""
}

func parseSelectColumns(fields []string) []string {
	start := indexOf(fields, "select")
	end := indexOf(fields, "from")
	if start == -1 || end == -1 || end <= start+1 {
		return []string{"*"}
	}
	cols := make([]string, 0, end-start-1)
	for _, token := range fields[start+1 : end] {
		parts := strings.Split(token, ",")
		for _, part := range parts {
			name := strings.TrimSpace(part)
			if name == "" {
				continue
			}
			cols = append(cols, name)
		}
	}
	if len(cols) == 0 {
		return []string{"*"}
	}
	return cols
}

func parseFilters(fields []string) (*int32, *int64, *int64, error) {
	whereIdx := indexOf(fields, "where")
	if whereIdx == -1 {
		return nil, nil, nil, nil
	}

	var partition *int32
	var offsetMin *int64
	var offsetMax *int64
	for i := whereIdx + 1; i < len(fields); i++ {
		switch fields[i] {
		case "limit", "last", "tail", "within", "scan":
			return partition, offsetMin, offsetMax, nil
		case "and":
			continue
		case "_partition":
			if i+2 >= len(fields) || fields[i+1] != "=" {
				return nil, nil, nil, fmt.Errorf("unsupported partition filter")
			}
			value, err := parseInt32(fields[i+2])
			if err != nil {
				return nil, nil, nil, fmt.Errorf("invalid partition value")
			}
			partition = &value
			i += 2
		case "_offset":
			if i+2 >= len(fields) {
				return nil, nil, nil, fmt.Errorf("unsupported offset filter")
			}
			op := fields[i+1]
			value, err := parseInt64(fields[i+2])
			if err != nil {
				return nil, nil, nil, fmt.Errorf("invalid offset value")
			}
			switch op {
			case ">=":
				offsetMin = &value
			case "<=":
				offsetMax = &value
			default:
				return nil, nil, nil, fmt.Errorf("unsupported offset operator")
			}
			i += 2
		default:
			return nil, nil, nil, fmt.Errorf("unsupported where clause")
		}
	}
	return partition, offsetMin, offsetMax, nil
}

func indexOf(fields []string, token string) int {
	for i, field := range fields {
		if field == token {
			return i
		}
	}
	return -1
}

func parseInt32(raw string) (int32, error) {
	value, err := strconv.ParseInt(raw, 10, 32)
	if err != nil {
		return 0, err
	}
	return int32(value), nil
}

func parseInt64(raw string) (int64, error) {
	value, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return 0, err
	}
	return value, nil
}

func hasToken(fields []string, token string) bool {
	for _, field := range fields {
		if field == token {
			return true
		}
	}
	return false
}
