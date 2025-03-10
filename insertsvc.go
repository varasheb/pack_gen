package main

import (
	"database/sql"
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	_ "github.com/lib/pq"
)

func InsertPackage(db *sql.DB, pkg Package) error {
	query := `
INSERT INTO "Package".packages ( groupid, modelid, groupname, modelname, hardwareversion, networkprovider, mainfirmware, coprocfirmware, mainsettingsid, plsign,coprocsettingname, updatedby)
VALUES (  $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12);
`
	// ON CONFLICT (groupname, modelname, hardwareversion, networkprovider) DO NOTHING;
	createdBy := "varashebkanthi@intellicar.in"
	_, err := db.Exec(query,
		pkg.GroupId,
		pkg.ModelId,
		pkg.GroupNames,
		pkg.Model,
		pkg.HWversion,
		pkg.SIM,
		pkg.LAFFirmware,
		pkg.CANFirmware,
		pkg.IOTSettingsSigned,
		pkg.PLSign,
		pkg.CoprocSetting,
		createdBy,
	)
	return err
}
func ResetSequence(db *sql.DB) error {
	query := `
    SELECT setval(
        pg_get_serial_sequence('"Package"."packages"', 'packagecode'), 
        COALESCE(MAX(packagecode), 1)
    ) FROM "Package"."packages";
    `
	_, err := db.Exec(query)
	if err != nil {
		return fmt.Errorf("failed to reset sequence: %v", err)
	}
	return nil
}
func InsertDb(Packages []*Package) {
	dsn := "postgresql://testing_owner:rilHO3obSc7X@ep-weathered-credit-a1p4w5k1.ap-southeast-1.aws.neon.tech/Dmt?sslmode=require"

	db, err := sql.Open("postgres", dsn)
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}
	defer db.Close()

	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(10 * time.Minute)

	if err := db.Ping(); err != nil {
		log.Fatalf("Database connection failed: %v", err)
	}

	if err := BackupAndClearDb(db); err != nil {
		log.Printf("Warning during backup and clear: %v", err)
	}

	numWorkers := 10
	packageChan := make(chan *Package, len(Packages))
	var wg sync.WaitGroup

	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go worker(db, packageChan, &wg)
	}

	for _, pkg := range Packages {
		packageChan <- pkg
	}
	close(packageChan)

	wg.Wait()
	fmt.Println("Package data successfully inserted into the database.")
}

func worker(db *sql.DB, packageChan <-chan *Package, wg *sync.WaitGroup) {
	defer wg.Done()
	if err := ResetSequence(db); err != nil {
		log.Printf("Failed to reset sequence: %v", err)
		return
	}
	for pkg := range packageChan {
		if err := InsertPackage(db, *pkg); err != nil {
			log.Printf("Failed to insert package: %v, ERROR: %v", pkg, err)
		}
	}
}

func BackupAndClearDb(db *sql.DB) error {
	rows, err := db.Query(`SELECT * FROM "Package".packages`)
	if err != nil {
		return fmt.Errorf("failed to fetch data for backup: %v", err)
	}
	defer rows.Close()

	currentDate := time.Now().Format("2006_01_02")
	folderPath := "./backups/"
	backupFileName := fmt.Sprintf("%s/package_backup_%s.csv", folderPath, currentDate)

	if err := os.MkdirAll(folderPath, os.ModePerm); err != nil {
		return fmt.Errorf("failed to create folder: %v", err)
	}

	file, err := os.Create(backupFileName)
	if err != nil {
		return fmt.Errorf("failed to create backup CSV: %v", err)
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	columns, err := rows.Columns()
	if err != nil {
		return fmt.Errorf("failed to fetch column names: %v", err)
	}
	if err := writer.Write(columns); err != nil {
		return fmt.Errorf("failed to write header to CSV: %v", err)
	}

	for rows.Next() {
		values := make([]interface{}, len(columns))
		valuePointers := make([]interface{}, len(columns))
		for i := range values {
			valuePointers[i] = &values[i]
		}

		if err := rows.Scan(valuePointers...); err != nil {
			return fmt.Errorf("failed to scan row: %v", err)
		}

		strValues := make([]string, len(values))
		for i, value := range values {
			if value == nil {
				strValues[i] = ""
			} else {
				strValues[i] = fmt.Sprintf("%v", value)
			}
		}

		if err := writer.Write(strValues); err != nil {
			return fmt.Errorf("failed to write row to CSV: %v", err)
		}
	}

	if _, err := db.Exec(`DELETE FROM "Package".packages`); err != nil {
		return fmt.Errorf("failed to clear the table: %v", err)
	}

	log.Printf("Backup created successfully at %s", backupFileName)
	return nil
}
