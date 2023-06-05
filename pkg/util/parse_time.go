/*
Copyright 2021 The KEDA Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package util

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"
)

type ParsedResult struct {
	Interval string
	Targets  []string
}

func convertStrToIntSlice(strSlice []string) ([]int, error) {
	intSlice := make([]int, len(strSlice))
	for i, str := range strSlice {
		num, err := strconv.Atoi(str)
		if err != nil {
			return nil, err
		}
		intSlice[i] = num
	}
	return intSlice, nil
}

func ParseTime(str string) (time.Time, error) {
	timeFormats := []string{
		"3:04 PM",
		"3:04 PM",
		"3:04 p.m.",
		"3:04 p.m",
		"3:04PM",
		"3:04pm",
		"15:04",
		"15:04 PM",
		"15:04 PM",
		"15:04 p.m.",
		"15:04 p.m",
		"15:04PM",
		"15:04pm",
	}

	var parsedTime time.Time
	var err error

	for _, layout := range timeFormats {
		parsedTime, err = time.Parse(layout, str)
		if err == nil {
			return parsedTime, nil
		}
	}

	return parsedTime, fmt.Errorf("unable to parse time: %s", str)
}

func ParseInterval(interval string) (ParsedResult, error) {
	// Match the days of the week pattern
	daysOfWeekPattern := regexp.MustCompile(`(Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday)`)
	daysOfWeekMatches := daysOfWeekPattern.FindAllString(interval, -1)

	// Match the days of the month pattern
	daysOfMonthPattern := regexp.MustCompile(`(\d+)`)
	daysOfMonthMatches := daysOfMonthPattern.FindAllString(interval, -1)

	// Extract the schedule frequency
	if strings.Contains(interval, "weekday") {
		return ParsedResult{Interval: "week", Targets: []string{"Monday", "Tuesday", "Wednesday", "Thursday", "Friday"}}, nil
	}
	if strings.Contains(interval, "weekend") {
		return ParsedResult{Interval: "week", Targets: []string{"Saturday", "Sunday"}}, nil
	}
	if strings.Contains(interval, "week") {
		if len(daysOfWeekMatches) > 0 {
			return ParsedResult{Interval: "week", Targets: []string{strings.Join(daysOfWeekMatches, ",")}}, nil
		}
		return ParsedResult{Interval: "week", Targets: []string{"Monday"}}, nil
	}
	if strings.Contains(interval, "day") {
		return ParsedResult{Interval: "day", Targets: []string{}}, nil
	}
	if strings.Contains(interval, "month") {
		if len(daysOfMonthMatches) > 0 {
			return ParsedResult{Interval: "month", Targets: []string{strings.Join(daysOfMonthMatches, ",")}}, nil
		}
		return ParsedResult{Interval: "month", Targets: []string{"1"}}, nil
	}

	return ParsedResult{}, fmt.Errorf("unable to parse period: %s", interval)
}

func getNextWeekday(weekdays []string) (time.Weekday, error) {
	today := time.Now().Weekday()
	for _, weekdayStr := range weekdays {
		weekday, err := parseWeekday(weekdayStr)
		if err != nil {
			return 0, err
		}
		if weekday > today {
			return weekday, nil
		}
	}
	// If no next weekday is found in the array, return the first weekday in the array
	firstWeekday, err := parseWeekday(weekdays[0])
	if err != nil {
		return 0, err
	}
	return firstWeekday, nil
}

func parseWeekday(weekdayStr string) (time.Weekday, error) {
	switch strings.ToLower(weekdayStr) {
	case "sunday":
		return time.Sunday, nil
	case "monday":
		return time.Monday, nil
	case "tuesday":
		return time.Tuesday, nil
	case "wednesday":
		return time.Wednesday, nil
	case "thursday":
		return time.Thursday, nil
	case "friday":
		return time.Friday, nil
	case "saturday":
		return time.Saturday, nil
	default:
		return 0, fmt.Errorf("invalid weekday: %s", weekdayStr)
	}
}

func getNextDayOfMonth(daysStr []string) (int, error) {
	days, _ := convertStrToIntSlice(daysStr)
	today := time.Now().Day()
	for _, day := range days {
		if day > today {
			return day, nil
		}
	}
	// If no next day is found in the array, return the first day in the array
	return days[0], nil
}

func parseNextTime(interval *ParsedResult, requiredTime *time.Time, location *time.Location) (time.Time, error) {
	currentTime := time.Now()
	if interval.Interval == "day" {
		return time.Date(currentTime.Year(), currentTime.Month(), currentTime.Day(), requiredTime.Hour(), requiredTime.Minute(), requiredTime.Second(), requiredTime.Nanosecond(), location), nil
	}
	if interval.Interval == "week" {
		getNextWeekday(interval.Targets)
		return time.Date(currentTime.Year(), currentTime.Month(), currentTime.Day(), requiredTime.Hour(), requiredTime.Minute(), requiredTime.Second(), requiredTime.Nanosecond(), location), nil
	}
	if interval.Interval == "month" {
		nextDay, err := getNextDayOfMonth(interval.Targets)
		if err != nil {
			return time.Time{}, err
		}
		return time.Date(currentTime.Year(), currentTime.Month(), nextDay, requiredTime.Hour(), requiredTime.Minute(), requiredTime.Second(), requiredTime.Nanosecond(), location), nil
	}
	return time.Time{}, fmt.Errorf("invalid data: %s", interval) // Make more convinient error
}
