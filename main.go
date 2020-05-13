package main

import (
	"crypto/tls"
	"encoding/json"
	"fmt"
	"github.com/microlib/simple"
	"gopkg.in/couchbase/gocb.v1"
	"io/ioutil"
	"net/http"
	"os"
	"strconv"
)

var (
	logger simple.Logger
)

const (
	MSGFORMAT     string = "function %s : %v\n"
	DBSETUP       string = "DBSetup"
	DBMIGRATE     string = "DBMigrate"
	AFFILIATE     string = "affiliate"
	AFFILIATES    string = "affiliates"
	AFFILIATEID   string = "affiliateid"
	PUBLICATIONS  string = "publications"
	PUBLICATIONID string = "publicationid"
	STOCKS        string = "stocks"
	SYMBOL        string = "symbol"
	STATUS        string = "status"
	MERGEDDATA    string = " : merged data"
	DATA          string = " : data"
	PERCENT       string = " percent"
)

func fp(msg string, obj interface{}) string {
	return fmt.Sprintf(MSGFORMAT, msg, obj)
}

func main() {
	logger.Level = os.Getenv("LOG_LEVEL")

	logger.Debug(fp("Envars ", os.Getenv("DB_URL")+" "+os.Getenv("DB_USER")+" "+os.Getenv("DB_PASSWORD")))

	cluster, _ := gocb.Connect(os.Getenv("DB_URL"))
	cluster.Authenticate(gocb.PasswordAuthenticator{
		Username: os.Getenv("DB_USER"),
		Password: os.Getenv("DB_PASSWORD"),
	})

	// we have a bucket for each affiliate
	bucket, _ := cluster.OpenBucket("Portfolio", "")
	defer bucket.Close()

	bucket.Manager("", "").CreatePrimaryIndex("", true, false)
	var affiliates []Affiliate
	var publications []Publication
	var publicationDetail PublicationDetail
	// var positions []Position
	var positionDetail []PositionDetail

	// do a lookup to get affiliate token on DB
	_, err := bucket.Get("affiliates", &affiliates)
	if err != nil {
		logger.Error(fp(DBMIGRATE+" : finding affiliate", err))
		os.Exit(-1)
	}
	logger.Info(fmt.Sprintf("Affiliates %v\n", affiliates))

	// set up http object
	tr := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}
	httpClient := &http.Client{Transport: tr}

	// do the api call to get Publications
	url := os.Getenv("URL")

	for y, _ := range affiliates {
		req, err := http.NewRequest("GET", url+"ApiPortfolio/GetAllPortfolios/?ApiKey="+affiliates[y].Token, nil)
		logger.Info(fp("DBMigrate URL info", url+"ApiPortfolio/GetAllPortfolios/?ApiKey="+affiliates[y].Token))
		resp, err := httpClient.Do(req)
		logger.Info(fmt.Sprintf("Retrieving all publication for affiliate %s %d", affiliates[y].Name, affiliates[y].Id))
		if err != nil || resp.StatusCode != 200 {
			logger.Error(fp(DBMIGRATE, err))
			os.Exit(-1)
		}
		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			logger.Error(fp(DBMIGRATE, err))
			os.Exit(-1)
		}

		json.Unmarshal(body, &publications)
		var items []gocb.BulkOp
		for x, _ := range publications {
			logger.Debug(fmt.Sprintf("Publications info %d", publications[x].UID))
			publications[x].AffiliateId = affiliates[y].Id
			// get the detailed portfolio data
			req, err := http.NewRequest("GET", url+"ApiPortfolio/Get?ApiKey="+affiliates[y].Token+"&id="+strconv.Itoa(publications[x].UID), nil)
			logger.Debug(fp("DBMigrate URL info", url+"ApiPortfolio/Get?ApiKey="+affiliates[y].Token+"&id="+strconv.Itoa(publications[x].UID)))
			resp, err := httpClient.Do(req)
			logger.Info(fp("DBMigrate retrieving portfolio details", publications[x].Name))
			if err != nil || resp.StatusCode != 200 {
				logger.Error(fp("DBMigrate retrieving portfolio info", err))
				os.Exit(-1)
			}
			defer resp.Body.Close()
			body, err := ioutil.ReadAll(resp.Body)
			if err != nil {
				logger.Error(fp(DBMIGRATE, err))
				os.Exit(-1)
			}
			logger.Debug(fmt.Sprintf("DBMigrate json data from url %s", string(body)))
			err = json.Unmarshal(body, &publicationDetail)
			if err != nil {
				logger.Error(fp(DBMIGRATE, err))
				os.Exit(-1)
			}

			// update items for bulk update
			publications[x].Data = publicationDetail
			publications[x].Type = "publication"
			items = append(items, &gocb.InsertOp{Key: "publication-" + strconv.Itoa(affiliates[y].Id) + "-" + strconv.Itoa(publications[x].UID), Value: &publications[x]})
		}
		err = bucket.Do(items)
		if err != nil {
			logger.Error(fp(DBMIGRATE, err))
			os.Exit(-1)
		}

		// get positions
		for x, _ := range publications {
			logger.Debug(fmt.Sprintf("Publications info %d", publications[x].UID))
			publications[x].AffiliateId = affiliates[y].Id
			// get the detailed portfolio data
			req, err := http.NewRequest("GET", url+"ApiPosition/GetAllByPortfolioId?ApiKey="+affiliates[y].Token+"&portfolioId="+strconv.Itoa(publications[x].UID), nil)
			logger.Debug(fp("DBMigrate URL info", url+"ApiPosition/GetAllByPortfolioId?ApiKey="+affiliates[y].Token+"&portfolioId="+strconv.Itoa(publications[x].UID)))
			resp, err := httpClient.Do(req)
			logger.Info(fp("DBMigrate retrieving position details", publications[x].Name))
			if err != nil || resp.StatusCode != 200 {
				logger.Error(fp("DBMigrate retrieving position info", err))
				os.Exit(-1)
			}
			defer resp.Body.Close()
			body, err := ioutil.ReadAll(resp.Body)
			if err != nil {
				logger.Error(fp(DBMIGRATE, err))
				os.Exit(-1)
			}
			logger.Debug(fmt.Sprintf("DBMigrate json data from url %s", string(body)))
			err = json.Unmarshal(body, &positionDetail)
			if err != nil {
				logger.Error(fp(DBMIGRATE, err))
				os.Exit(-1)
			}

			// update items for bulk update
			position := Position{UID: publications[x].UID, Name: publications[x].Name, Type: "position", Data: positionDetail}
			publications[x].Type = "position"
			items = append(items, &gocb.InsertOp{Key: "position-" + strconv.Itoa(affiliates[y].Id) + "-" + strconv.Itoa(publications[x].UID), Value: &position})

		}
		err = bucket.Do(items)
		if err != nil {
			logger.Error(fp(DBMIGRATE, err))
			os.Exit(-1)
		}
	}
}
