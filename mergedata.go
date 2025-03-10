package main

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/fabrikiot/wsmqttrt/wsmqttrtpuller"
)

type Record struct {
	LAFFirmware       string
	CANFirmware       string
	HWVersion         string
	Sim               string
	IOTSettingsSigned string
	LAFSetting        string
	CoprocSetting     string
	PLSign            string
}

var mu sync.Mutex

// func main() {
// 	alldata := getallmergeData()
// 	for key, val := range alldata {
// 		fmt.Println(key, ",", val.LAFFirmware, ",", val.CANFirmware, ",", val.HWVersion, ",", val.Sim, ",", val.PLSign, ",", val.IOTSettingsSigned)
// 	}
// }

func GetallmergeData() map[string]*Record {
	var RecordMap = make(map[string]*Record)

	fmt.Println("Config updates script started...")

	wspulleropts := wsmqttrtpuller.NewWsMqttRtPullerOpts("dmt1.intellicar.in", 11884)
	wg := &sync.WaitGroup{}

	stoppedflag := uint32(0)
	var once sync.Once

	statecallback := &wsmqttrtpuller.WsMqttRtPullerStateCallback{
		Started: func() {
			// Signal that the puller has started
			fmt.Println("Puller started")
		},
		Stopped: func() {
			once.Do(func() {
				atomic.StoreUint32(&stoppedflag, 1)
				fmt.Println("Puller stopped")
			})
		},
	}

	subscribecallback := func(topic []byte, issubscribe bool, isok bool) {
		if isok {
			fmt.Printf("Subscribed to topic: %s\n", string(topic))
		} else {
			fmt.Printf("Failed to subscribe to topic: %s\n", string(topic))
		}
	}

	//------------ Msg callback for handling incoming MQTT messages ------------
	msgcallback := func(topic []byte, payload []byte) {
		topics := strings.Split(string(topic), "/")
		deviceid := topics[len(topics)-1]
		topictype := topics[len(topics)-2]

		mu.Lock()
		defer mu.Unlock()

		record, exists := RecordMap[deviceid]
		if !exists {
			record = &Record{}
		}

		if topictype == "coprocstatus" {
			coproc_payload := make(map[string]interface{})

			if err := json.Unmarshal(payload, &coproc_payload); err != nil {
				log.Println("Error unmarshalling payload:", err)
				return
			}

			if coprocStatusInfo, ok := coproc_payload["coprocStatusInfo"].(map[string]interface{}); ok {
				if hexValue, ok := coprocStatusInfo["4"].(string); ok {
					asciiValue := hexToASCII(hexValue)
					record.CANFirmware = asciiValue
				} else {
					log.Printf("Device ID: %s, Coproc Setting: Key '4' not found\n", deviceid)
				}
				if plsign, ok := coprocStatusInfo["5"].(string); ok {
					record.PLSign = plsign
				}
			} else {
				log.Printf("Device ID: %s, Coproc Status Info: Not available\n", deviceid)
			}
		}

		if topictype == "deviceinfo" {
			deviceInfo := make(map[string]interface{})

			if err := json.Unmarshal(payload, &deviceInfo); err != nil {
				log.Println("Error unmarshalling deviceinfo payload:", err)
				return
			}

			// if sim, ok := deviceInfo["13"].(string); ok {
			// 	record.Sim = strings.Trim(sim, "\"")
			// }
			if IOTSettingsSigned, ok := deviceInfo["15"].(string); ok {
				if len(IOTSettingsSigned) == 68 {
					//remove last 4 char
					IOTSettingsSigned = IOTSettingsSigned[:len(IOTSettingsSigned)-4]
				}
				record.IOTSettingsSigned = IOTSettingsSigned
			}
			if CoprocSetting, ok := deviceInfo["9"].(string); ok {
				if len(CoprocSetting) == 68 {
					//remove last 4 char
					CoprocSetting = CoprocSetting[:len(CoprocSetting)-4]
				}
				record.CoprocSetting = CoprocSetting
			}
			if laFirmware, ok := deviceInfo["2"].(string); ok {
				var sim2 string
				record.LAFFirmware = laFirmware
				record.HWVersion, sim2 = getHwvext(laFirmware)
				if record.Sim == "" {
					record.Sim = sim2
				}
			}

		}

		RecordMap[deviceid] = record
	}

	//-----------
	wspuller := wsmqttrtpuller.NewWsMqttRtPuller(wspulleropts, statecallback, msgcallback)
	wg.Add(1)
	go func() {
		defer wg.Done()
		wspuller.Start()
		for atomic.LoadUint32(&stoppedflag) == 0 {
			time.Sleep(100 * time.Millisecond)
		}
	}()

	// Topics to subscribe to
	Topics := []string{"/intellicar/layer5/coprocstatus/", "/intellicar/layer5/deviceinfo/"}
	subscribedTopics := make(map[string]bool)
	for _, eachtopic := range Topics {
		fullTopic := eachtopic + "+"
		if !subscribedTopics[fullTopic] {
			subscribedTopics[fullTopic] = true
			wspuller.Subscribe([]byte(fullTopic), subscribecallback)
		}
	}

	ossigch := make(chan os.Signal, 1)
	signal.Notify(ossigch, os.Interrupt, syscall.SIGTERM, syscall.SIGINT)

	select {
	case <-ossigch:
		fmt.Println("Interrupt signal received, stopping...")
	case <-time.After(2 * time.Minute):
		fmt.Println("Timeout reached, stopping...")
	}

	wspuller.Stop()
	wg.Wait()

	fmt.Println("Collected Record map:", len(RecordMap))
	return RecordMap
}

func hexToASCII(hexStr string) string {
	bytes, err := hex.DecodeString(hexStr)
	if err != nil {
		return getcoproc(hexStr)
	}
	asciiStr := string(bytes)
	return getcoproc(asciiStr)
}

func getcoproc(cset string) string {
	cset = strings.TrimRight(cset, "\u0000")
	cset = strings.Split(cset, ",")[0]
	return cset
}

// func getHwvext(input string) (string, string) {
// 	parts := strings.Split(input, "-")
// 	sim := "Airtel"
// 	if len(parts) > 6 {
// 		if parts[6] == "JIO2" {
// 			sim = "JIO"
// 		}
// 	}

// 	if len(parts) < 2 {
// 		part := strings.Split(input, "_")
// 		if part[0] == "LAF" && part[1] == "V1" {
// 			return part[1] + "_" + part[2], sim
// 		} else if part[0] == "LAF" && part[1] == "V2" {
// 			return part[1] + "_" + part[2], sim

// 		} else if part[0] == "LAF" {
// 			return part[2], sim
// 		} else if part[0] == "LAFM" && part[1] == "4G" {
// 			return part[2], sim
// 		} else if part[0] == "LAFM" {
// 			return part[2], sim
// 		} else if part[0] == "LA5" {
// 			return part[2] + "_" + part[3], sim
// 		}
// 		log.Printf("Invalid input format: %s\n", input)
// 		return "NA", sim
// 	}
// 	var HDVersion string
// 	if parts[1] == "4G_EC200UCN" || parts[1] == "4G_EG21G" {

//			HDVersion = parts[2]
//			return HDVersion, sim
//		} else if parts[1] == "2G_MC60" {
//			HDVersion = parts[2]
//			return HDVersion, sim
//		} else if parts[1] == "4G_EC200UEU" {
//			return parts[2], sim
//		} else if parts[1] == "2G_MC60_SFF" {
//			return "SFF", sim
//		} else {
//			return "NA", sim
//		}
//	}

// func getHwvext(input string) (string, string) {
// 	parts := strings.Split(input, "-")
// 	sim := "Airtel"

// 	if len(parts) > 6 {
// 		if parts[6] == "JIO2" {
// 			sim = "JIO"
// 		}

// 	}
// 	if strings.Contains(input, "UG_ONO") {
// 		sim = "UG_ONO"
// 	} else if strings.Contains(input, "US_ONO") {
// 		sim = "US_ONO"
// 	} else if strings.Contains(input, "NG_ONO") {
// 		sim = "NG_ONO"
// 	} else if strings.Contains(input, "MY_ONO") {
// 		sim = "MY_ONO"
// 	} else if strings.Contains(input, "ONO") {
// 		sim = "ONO"
// 	} else if strings.Contains(input, "GREAVES_TATA") && strings.Contains(input, "TATA") {
// 		sim = "GREAVES_TATA"
// 	} else if strings.Contains(input, "TAISYS") {
// 		sim = "TAISYS"
// 	} else if strings.Contains(input, "SENS") {
// 		sim = "SENS"
// 	} else if strings.Contains(input, "UAE_HOLO") {
// 		sim = "UAE_HOLO"
// 	} else if strings.Contains(input, "MY_HOLO") {
// 		sim = "MY_HOLO"
// 	} else if strings.Contains(input, "US_HOLO") {
// 		sim = "US_HOLO"
// 	} else if strings.Contains(input, "HOLO") {
// 		sim = "HOLO"
// 	} else if strings.Contains(input, "S_MY") || strings.Contains(input, "S-MY") {
// 		sim = "S_MY"
// 	} else if strings.Contains(input, "_AU") && !strings.Contains(input, "AUG") {
// 		sim = "AU"
// 	}
// 	if len(parts) < 2 {
// 		part := strings.Split(input, "_")
// 		if part[0] == "LAF" && part[1] == "V1" {
// 			return part[1] + part[2], sim
// 		} else if part[0] == "LAF" && part[1] == "V2" {
// 			if len(part[3]) >= 2 {
// 				return part[1] + part[2], sim
// 			}
// 			return part[1] + part[2] + part[3], sim

// 		} else if part[0] == "LAF" && part[1] == "SFF" {
// 			return part[1] + part[2] + part[3], sim

// 		} else if part[0] == "LAF" {
// 			return part[2], sim
// 		} else if part[0] == "LAFM" && part[1] == "4G" {
// 			if _, err := strconv.Atoi(part[3]); err == nil {
// 				return part[2] + part[3], sim
// 			}
// 			return part[2], sim
// 		} else if part[0] == "LAFM" {
// 			if _, err := strconv.Atoi(part[3]); err == nil {
// 				return part[2] + part[3], sim
// 			}
// 			return part[2], sim
// 		} else if part[0] == "LA5" {
// 			return part[2] + part[3], sim
// 		}
// 		log.Printf("Invalid input format: %s\n", input)
// 		return "NA", sim
// 	}
// 	var HDVersion string
// 	if parts[1] == "4G_EC200UCN" || parts[1] == "4G_EG21G" {

// 		HDVersion = parts[2]
// 		return HDVersion, sim
// 	} else if parts[1] == "2G_MC60" {
// 		HDVersion = parts[2]
// 		return HDVersion, sim
// 	} else if parts[1] == "4G_EC200UEU" {
// 		return parts[2], sim
// 	} else if parts[1] == "2G_MC60_SFF" {
// 		return "SFF", sim
// 	} else {
// 		return "NA", sim
// 	}
// }

func getHwvext(input string) (string, string) {
	parts := strings.Split(input, "-")
	sim := "AIRTEL"

	if len(parts) > 6 {
		if parts[6] == "JIO2" {
			sim = "JIO2"
		} else if parts[6] != "BETA" {
			sim = parts[6]
		}

	}
	if strings.Contains(input, "S_MY") || strings.Contains(input, "S-MY") {
		sim = "S_MY"
	} else if strings.Contains(input, "_AU") && !strings.Contains(input, "AUG") {
		sim = "AU"
	}

	if strings.Contains(input, "PCB_IOT_NRF_SM") {
		return "PCB-IOT-NRF-SM-30", sim
	}
	if strings.Contains(input, "LAFM_MINI_LG_V1_2") {
		return "MINI-LG-V12", sim
	}
	if len(parts) < 2 {
		part := strings.Split(input, "_")
		// LAF
		if part[0] == "LAF" && part[1] == "V1" {
			return part[1] + part[2], sim
		} else if part[0] == "LAF" && part[1] == "V2" {
			if len(part[3]) >= 2 {
				return part[1] + part[2], sim
			}
			return part[1] + part[2] + part[3], sim

		} else if part[0] == "LAF" && part[1] == "SFF" {
			return part[1] + "-V1", sim

		} else if part[0] == "LAF" {
			return part[2], sim
		}
		// LAFM
		if part[0] == "LAFM" && part[1] == "4G" {
			if _, err := strconv.Atoi(part[3]); err == nil {
				return part[2] + part[3], sim
			}
			return part[2], sim
		} else if part[0] == "LAFM" && part[1] == "MINI" {
			if _, err := strconv.Atoi(part[3]); err == nil {
				return part[1] + "-" + part[2] + part[3], sim
			}
			return part[1] + part[2], sim
		} else if part[0] == "LAFM" && part[1] == "2G" {
			return "SFF-V2", sim
		} else if part[0] == "LAFM" {
			if _, err := strconv.Atoi(part[3]); err == nil {
				return part[2] + part[3], sim
			} else if part[3] == "SFF" {
				return part[3], sim
			}
			return part[2], sim
		} else if part[0] == "LA5" {
			return "BASE-V20-LAYER2-V20", sim
		}
	}
	if len(parts) >= 2 {
		if parts[1] == "4G_EC200UCN" || parts[1] == "4G_EG21G" {
			return parts[2], sim
		} else if parts[1] == "2G_MC60" {
			return parts[2] + "-V2", sim
		} else if parts[1] == "4G_EC200UEU" {
			return parts[2], sim
		} else if parts[1] == "2G_MC60_SFF" {
			return "SFF-V2", sim
		} else if parts[0] == "LAFMV2_4G" {
			part := strings.Split(parts[1], "_")
			return part[1], sim
		} else if parts[1] == "4G_EG21GL" {
			return parts[2], sim
		}
	}

	log.Printf("Invalid input format: %s\n", input)
	return "NA", "NA"
}
