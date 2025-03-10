package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
)

type Group struct {
	Name     string `json:"name"`
	GroupId  int    `json:"groupid"`
	Path     string `json:"path"`
	PGroupId int    `json:"pgroupid"`
	PName    string `json:"pname"`
	PPath    string `json:"ppath"`
}

type Groupdata struct {
	Status string  `json:"status"`
	Data   []Group `json:"data"`
	Err    string  `json:"err"`
	Msg    string  `json:"msg"`
}

type deviceResponse struct {
	Status string        `json:"status"`
	Data   []VehicleData `json:"data"`
	Err    string        `json:"err"`
	Msg    string        `json:"msg"`
}

type VehicleData struct {
	VehicleID       int             `json:"vehicleid"`
	VehicleNo       string          `json:"vehicleno"`
	Devices         []Device        `json:"devices"`
	GroupInfo       [][]interface{} `json:"groupInfo"`
	VehiclePrefData *string         `json:"vehicleprefdata"`
}

type Device struct {
	DeviceNo   string `json:"deviceno"`
	DeviceID   int    `json:"deviceid"`
	DeviceType string `json:"devicetype"`
	SimNo      string `json:"simno"`
	SimID      int    `json:"simid"`
	BindTag    string `json:"bindtag"`
}
type Modelinfo struct {
	ModelId      int    `json:"modelid"`
	Vehicletype  string `json:"vehicletype"`
	Oem          string `json:"oem"`
	Model        string `json:"model"`
	Variant      string `json:"variant"`
	Year         int    `json:"year"`
	Fueltype     string `json:"fueltype"`
	Transmission string `json:"transmission"`
}
type DeviceInfo struct {
	DeviceNo   string
	GroupId    int
	GroupNames []string
	Model      string
	ModelId    int
}
type Package struct {
	DeviceNo          string
	GroupId           int
	ModelId           int
	GroupNames        string
	Model             string
	SIM               string
	HWversion         string
	LAFFirmware       string
	CANFirmware       string
	PLSign            string
	IOTSettingsSigned string
	CoprocSetting     string
}

// var packagemap map[string]*Packages

func main() {
	pid := os.Getpid()
	fmt.Printf("Process ID: %d\n", pid)
	token := Gettoken()
	// fmt.Println(token)
	p := getPackages(token)
	// packages := p
	// Print CSV header
	fmt.Println("DeviceNo,GroupId,ModelId,Group,Model,SIM,HWversion,LAFFirmware,CANFirmware,IOTSettingsSigned,PLSign,CoprocSetting")

	for _, packge := range p {
		if len(packge.Model) > 10 {
			// Format each field and escape any commas by wrapping in quotes
			deviceNo := escapeCSV(packge.DeviceNo)
			groupNames := escapeCSV(packge.GroupNames)
			model := escapeCSV(packge.Model)
			sim := escapeCSV(packge.SIM)
			hwVersion := escapeCSV(packge.HWversion)
			lafFirmware := escapeCSV(packge.LAFFirmware)
			canFirmware := escapeCSV(packge.CANFirmware)
			iotSettings := escapeCSV(packge.IOTSettingsSigned)
			plSign := escapeCSV(packge.PLSign)
			coprocSetting := escapeCSV(packge.CoprocSetting)

			row := fmt.Sprintf("%s,%d,%d,%s,%s,%s,%s,%s,%s,%s,%s,%s",
				deviceNo,
				packge.GroupId,
				packge.ModelId,
				groupNames,
				model,
				sim,
				hwVersion,
				lafFirmware,
				canFirmware,
				plSign,
				iotSettings,
				coprocSetting)
			fmt.Println(row)
		}
	}

	pmap := make(map[string][]*Package)
	for _, pakI := range p {

		key := pakI.GroupNames + " / " + pakI.Model + " / " + pakI.SIM + " / " + pakI.HWversion
		pmap[key] = append(pmap[key], pakI)
	}
	var uniquepackage []*Package
	for _, v := range pmap {
		var sval Package
		sval = *v[0]
		for _, paI := range v {

			if len(paI.CANFirmware) > 10 && len(paI.IOTSettingsSigned) == 64 && len(paI.CoprocSetting) == 64 && len(paI.PLSign) == 64 {
				sval = *paI
			}

		}
		if sval.GroupNames != "" && sval.CANFirmware != "" {
			uniquepackage = append(uniquepackage, &sval)
		}
	}

	InsertDb(uniquepackage)
	fmt.Println("Sucefully Updated :)...........", len(uniquepackage), len(pmap))
	fmt.Println("Finised !!!!!!!!!!!!!!!!!!!")

}

func getPackages(token string) []*Package {
	var packagelist []*Package
	var devicelist []*DeviceInfo
	var mu sync.Mutex
	groups, err := Getmygroups(token)
	if err != nil {
		log.Fatalln("Error fatal:", err.Error())
	}

	numWorkers := 63
	taskQueue := make(chan Group, len(groups))
	var wg sync.WaitGroup
	worker := func() {
		for group := range taskQueue {
			vdsdata, err := GetmyDevice(token, group.GroupId)
			if err != nil {
				log.Println("Error getting device data:", err.Error())
				wg.Done()
				continue
			}
			for _, val := range vdsdata {
				if val.VehicleNo != "" && len(val.Devices) > 0 && val.Devices[0].DeviceType == "laf" {
					modeldata, modelid := getmodel(*val.VehiclePrefData)
					deviceNo := val.Devices[0].DeviceNo
					mu.Lock()
					// if existingDevice, exists := deviceMap[deviceNo]; exists {
					// 	if !contains(existingDevice.GroupNames, group.Name) {
					// 		existingDevice.GroupNames = append(existingDevice.GroupNames, group.Name)
					// 	}
					// } else {
					devicelist = append(devicelist, &DeviceInfo{
						DeviceNo:   deviceNo,
						GroupId:    group.GroupId,
						GroupNames: []string{group.Name},
						Model:      modeldata,
						ModelId:    modelid,
					})
					// }
					mu.Unlock()
				}
			}
			wg.Done()
		}
	}
	for i := 0; i < numWorkers; i++ {
		go worker()
	}
	for _, group := range groups {
		if group.PName == "fleet" && group.Name != "la5.ic" && group.Name != "LA5.IC.HARDWARE" && group.Name != "Demo_batt" {
			wg.Add(1)
			taskQueue <- group
		}
	}
	close(taskQueue)
	wg.Wait()
	fmt.Println("Getting Mqtt data...........")
	odata := GetallmergeData()

	if err != nil {
		log.Fatal("Error reading Optix data:", err)
	}

	fmt.Println("entering data to map.....")
	for _, device := range devicelist {
		deviceid := strings.TrimSpace(device.DeviceNo)
		if len(deviceid) == 16 {
			if optixRecord, found := odata[device.DeviceNo]; found {
				groupNames := strings.Join(device.GroupNames, " / ")
				packagelist = append(packagelist, &Package{
					DeviceNo:          deviceid,
					GroupId:           device.GroupId,
					ModelId:           device.ModelId,
					GroupNames:        groupNames,
					Model:             device.Model,
					SIM:               optixRecord.Sim,
					HWversion:         optixRecord.HWVersion,
					LAFFirmware:       optixRecord.LAFFirmware,
					CANFirmware:       optixRecord.CANFirmware,
					PLSign:            optixRecord.PLSign,
					IOTSettingsSigned: optixRecord.IOTSettingsSigned,
					CoprocSetting:     optixRecord.CoprocSetting,
				})
			}
		}
	}
	sort.Slice(packagelist, func(i, j int) bool {
		return strings.ToLower(packagelist[i].GroupNames) < strings.ToLower(packagelist[j].GroupNames)
	})

	fmt.Println("completed Data Fetching Starting Insertion........")
	// for key, val := range packagemap {
	// 	fmt.Println(key, ",", val.GroupNames, ",", val.Model, ",", val.SIM, ",", val.HWversion, ",", val.LAFFirmware, ",", val.CANFirmware)
	// }
	return packagelist
}

func getmodel(data string) (string, int) {
	var vehiclePrefData Modelinfo

	err := json.Unmarshal([]byte(data), &vehiclePrefData)
	if err != nil {
		log.Println("Error unmarshalling JSON:", err)
	}
	processValue := func(s string) string {
		s = strings.TrimSpace(s)
		if strings.Contains(s, ",") {
			return fmt.Sprintf("\"%s\"", s)
		}
		return s
	}
	str := []string{
		processValue(vehiclePrefData.Vehicletype),
		processValue(vehiclePrefData.Oem),
		processValue(vehiclePrefData.Model),
		processValue(vehiclePrefData.Variant),
		strconv.Itoa(vehiclePrefData.Year),
		processValue(vehiclePrefData.Fueltype),
		processValue(vehiclePrefData.Transmission),
	}

	return strings.Join(str, "_"), vehiclePrefData.ModelId
}

func Gettoken() string {
	postBody, _ := json.Marshal(map[string]map[string]string{
		"user": {
			"type":     "localuser",
			"username": "debug.admin",
			"password": "xyz321",
		}})

	responseBody := bytes.NewBuffer(postBody)
	resp, err := http.Post("https://apiplatform.intellicar.in/gettoken", "application/json", responseBody)

	if err != nil {
		log.Fatalf("An Error Occured %v", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Fatalln(err)
	}
	tokenResp := struct {
		Status string `json:"status"`
		Data   struct {
			Token string `json:"token"`
		} `json:"data"`
		Userinfo struct {
			Userid   int    `json:"userid"`
			Typeid   int    `json:"typeid"`
			Username string `json:"username"`
		} `json:"userinfo"`
		Err string `json:"err"`
		Msg string `json:"msg"`
	}{}
	if err := json.Unmarshal(body, &tokenResp); err != nil {
		log.Fatal("Token parsing error")
	}

	Token := tokenResp.Data.Token

	// fmt.Println(Token)
	return Token
}

func Getmygroups(Token string) ([]Group, error) {
	postBody, _ := json.Marshal(map[string]string{
		"token": Token,
	})

	requestBody := bytes.NewBuffer(postBody)

	resp, err := http.Post("https://apiplatform.intellicar.in/api/user/getmygroups", "application/json", requestBody)
	if err != nil {
		return nil, fmt.Errorf("error making POST request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("received non-OK HTTP status: %s", resp.Status)
	}

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("error reading response body: %v", err)
	}

	var responseData Groupdata
	err = json.Unmarshal(body, &responseData)
	if err != nil {
		return nil, fmt.Errorf("error unmarshalling JSON: %v", err)
	}

	if responseData.Status != "SUCCESS" {
		return nil, fmt.Errorf("API error: %v", responseData.Err)
	}

	return responseData.Data, nil
}

func GetmyDevice(Token string, Groupid int) ([]VehicleData, error) {

	PostBody, err := json.Marshal(map[string]string{
		"token":   Token,
		"groupid": fmt.Sprint(Groupid),
	})
	if err != nil {
		return nil, fmt.Errorf("error marshalling request body: %v", err)
	}

	ResponseBody := bytes.NewBuffer(PostBody)

	Resp, err := http.Post("http://apiplatform.intellicar.in/api/vehicle/getmyvdsnew", "application/json", ResponseBody)
	if err != nil {
		return nil, fmt.Errorf("an error occurred while making the POST request: %v", err)
	}
	defer Resp.Body.Close()

	if Resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("received non-OK HTTP status: %s", Resp.Status)
	}

	body, err := ioutil.ReadAll(Resp.Body)
	if err != nil {
		return nil, fmt.Errorf("error reading response body: %v", err)
	}
	var response deviceResponse
	err = json.Unmarshal(body, &response)
	if err != nil {
		log.Println("error1", err)
		return nil, fmt.Errorf("error unmarshalling JSON: %v", err)
	}

	if response.Status != "SUCCESS" {
		log.Println("error2")
		return nil, fmt.Errorf("API error: %v", response.Err)
	}
	return response.Data, nil
}

// func contains(slice []string, item string) bool {
// 	for _, v := range slice {
// 		if v == item {
// 			return true
// 		}
// 	}
// 	return false
// }

func escapeCSV(s string) string {
	if strings.Contains(s, ",") || strings.Contains(s, "\"") || strings.Contains(s, "\n") {
		s = strings.ReplaceAll(s, "\"", "\"\"")
		return fmt.Sprintf("\"%s\"", s)
	}
	return s
}
