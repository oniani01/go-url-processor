package main

import (
	"fmt"
	"math/rand"
	"sync"
	"time"
)

type Job struct {
	ID  int
	URL string
}

type Result struct {
	Job      Job
	Status   string
	Duration time.Duration
}

func worker(id int, jobs <-chan Job, results chan<- Result, wg *sync.WaitGroup) {

	defer wg.Done()

	for j := range jobs {
		fmt.Printf("[Воркер %d] Начал обработку Job ID: %d (%s)\n", id, j.ID, j.URL)

		startTime := time.Now()
		randomDelay := time.Duration(rand.Intn(1000)+100) * time.Millisecond
		time.Sleep(randomDelay)
		duration := time.Since(startTime)

		results <- Result{
			Job:      j,
			Status:   "обработан",
			Duration: duration,
		}

		fmt.Printf("[Воркер %d] Завершил обработку Job ID: %d за %v\n", id, j.ID, duration)
	}
}

func main() {

	urls := []string{
		"https://api.example.com/users",
		"https://api.example.com/posts",
		"https://api.example.com/comments",
		"https://api.example.com/albums",
		"https://api.example.com/photos",
		"https://api.example.com/todos",
		"https://api.example.com/settings",
		"https://api.example.com/profile",
		"https://api.example.com/dashboard",
		"https://api.example.com/analytics",
		"https://api.example.com/reports",
		"https://api.example.com/notifications",
	}

	numWorkers := 5

	jobs := make(chan Job, len(urls))
	results := make(chan Result, len(urls))

	var wg sync.WaitGroup

	for w := 1; w <= numWorkers; w++ {
		wg.Add(1)
		go worker(w, jobs, results, &wg)
	}

	for i, url := range urls {
		jobs <- Job{
			ID:  i + 1,
			URL: url,
		}
	}

	close(jobs)

	go func() {
		wg.Wait()
		close(results)
	}()

	var totalDuration time.Duration
	var successCount int
	var allResults []Result

	for r := range results {
		allResults = append(allResults, r)
		totalDuration += r.Duration
		if r.Status == "обработан" {
			successCount++
		}
	}

	fmt.Println("\n========================================")
	fmt.Println("          ИТОГОВЫЙ ОТЧЁТ")
	fmt.Println("========================================")

	for _, r := range allResults {
		fmt.Printf("Job ID: %02d | URL: %-35s | Статус: %-10s | Время: %v\n",
			r.Job.ID, r.Job.URL, r.Status, r.Duration)
	}

	fmt.Println("----------------------------------------")
	fmt.Printf("Всего обработано URL: %d\n", len(allResults))
	fmt.Printf("Успешных операций:    %d\n", successCount)

	if len(allResults) > 0 {
		avgDuration := totalDuration / time.Duration(len(allResults))
		fmt.Printf("Суммарное время:      %v\n", totalDuration)
		fmt.Printf("Среднее время (1 URL): %v\n", avgDuration)
	}
	fmt.Println("========================================")
}
