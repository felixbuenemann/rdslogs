package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/rds"
)

func parseLogTime(line string, engine string) (time.Time, error) {
	switch engine {
	case "mysql", "mariadb":
		parts := strings.Split(line, " ")
		if len(parts) < 1 {
			return time.Time{}, fmt.Errorf("invalid format")
		}
		return time.Parse("2006-01-02T15:04:05.999999Z", parts[0])
	case "postgres":
		timeStr := strings.Split(line, " UTC")[0]
		return time.Parse("2006-01-02 15:04:05", timeStr)
	default:
		return time.Time{}, fmt.Errorf("unsupported engine: %s", engine)
	}
}

func getDBEngine(ctx context.Context, client *rds.Client, identifier string) (string, error) {
	input := &rds.DescribeDBInstancesInput{
		DBInstanceIdentifier: &identifier,
	}
	output, err := client.DescribeDBInstances(ctx, input)
	if err != nil {
		return "", fmt.Errorf("failed to describe DB instance: %v", err)
	}
	if len(output.DBInstances) == 0 {
		return "", fmt.Errorf("DB instance not found")
	}
	return *output.DBInstances[0].Engine, nil
}

func main() {
	instance := flag.String("instance", "", "RDS instance identifier")
	since := flag.String("since", "", "Start from logs after this timestamp (format: 2006-01-02 15:04:05) or duration (1h, 5m)")
	follow := flag.Bool("f", false, "Follow log output")
	flag.BoolVar(follow, "follow", false, "Follow log output")
	flag.Parse()

	if *instance == "" {
		fmt.Println("--instance is required")
		os.Exit(1)
	}

	cfg, err := config.LoadDefaultConfig(context.Background())
	if err != nil {
		fmt.Printf("Unable to load SDK config: %v\n", err)
		os.Exit(1)
	}

	client := rds.NewFromConfig(cfg)
	ctx := context.Background()

	engine, err := getDBEngine(ctx, client, *instance)
	if err != nil {
		fmt.Printf("Failed to detect engine: %v\n", err)
		os.Exit(1)
	}

	lastMarkers := make(map[string]string)
	var filterTime time.Time
	if *since != "" {
		if d, err := time.ParseDuration(*since); err == nil {
			filterTime = time.Now().UTC().Add(-d)
		} else {
			filterTime, err = time.Parse("2006-01-02 15:04:05", *since)
			if err != nil {
				filterTime, err = time.Parse(time.RFC3339, *since)
				if err != nil {
					fmt.Printf("Invalid format. Use RFC3339 (2006-01-02T15:04:05Z), timestamp (2006-01-02 15:04:05) or duration (1h, 5m)\n")
					os.Exit(1)
				}
			}
			filterTime = filterTime.UTC()
		}
	}

	for {
		input := &rds.DescribeDBLogFilesInput{
			DBInstanceIdentifier: instance,
		}
		if *since != "" {
			input.FileLastWritten = aws.Int64(filterTime.UnixMilli())
		}

		output, err := client.DescribeDBLogFiles(ctx, input)
		if err != nil {
			fmt.Printf("Failed to describe log files: %v\n", err)
			os.Exit(1)
		}

		sort.Slice(output.DescribeDBLogFiles, func(i, j int) bool {
			return *output.DescribeDBLogFiles[i].LastWritten < *output.DescribeDBLogFiles[j].LastWritten
		})

		for _, file := range output.DescribeDBLogFiles {
			downloadInput := &rds.DownloadDBLogFilePortionInput{
				DBInstanceIdentifier: instance,
				LogFileName:          file.LogFileName,
			}

			if marker := lastMarkers[*file.LogFileName]; marker != "" {
				downloadInput.Marker = &marker
			}

			var lastMarker string
			for {
				portion, err := client.DownloadDBLogFilePortion(ctx, downloadInput)
				if err != nil {
					fmt.Printf("Failed to download log portion: %v\n", err)
					break
				}

				if portion.LogFileData != nil {
					lines := strings.Split(*portion.LogFileData, "\n")
					for _, line := range lines {
						if strings.TrimSpace(line) == "" {
							continue
						}
						lineTime, err := parseLogTime(line, engine)
						if err != nil {
							fmt.Print(line + "\n")
							continue
						}
						if *since == "" || lineTime.After(filterTime) {
							fmt.Print(line + "\n")
						}
					}
				}

				if portion.Marker != nil {
					lastMarker = *portion.Marker
					downloadInput.Marker = &lastMarker
				}

				if !*portion.AdditionalDataPending {
					break
				}
			}

			if lastMarker != "" {
				lastMarkers[*file.LogFileName] = lastMarker
			}
		}

		if !*follow {
			break
		}

		time.Sleep(5 * time.Second)
	}
}
