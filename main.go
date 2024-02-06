package main

import (
	"database/sql"
	"fmt"
	_ "github.com/lib/pq"
	"log"
	"strings"
)

type Department struct {
	departmentName string
}

type Employee struct {
	employeeName string
	departmentID int
}

func main() {
	connStr := "postgres://postgres.bqmpwhiumdnruxxhwzur:SuperSandBox!@aws-0-ap-southeast-1.pooler.supabase.com:5432/postgres"
	db, err := sql.Open("postgres", connStr)

	defer db.Close()

	if err != nil {
		log.Fatalln(err)
	}

	if err = db.Ping(); err != nil {
		log.Fatalln(err)
	}

	//Tugas 1
	//createDepartmentTable(db)
	//createEmployeeTable(db)

	//Tugas 2
	//department := Department{"Finance"}
	//pkd := insertDepartment(db,department)
	//fmt.Printf("ID = %d \n", pkd)

	//pk := insertEmployee( db, Employee{"Fifeka",pkd})
	//fmt.Println("employee ID =", pk)

	//pk := insertEmployee( db, Employee{"Onanda",pkd})
	//fmt.Println("employee ID =", pk)

	//pk  := insertDepartment(db,Department{"Engineering"})
	//fmt.Println("Total Updated Data", updateEmployeeDepartment(db,pk,1))

	//fmt.Println(getAllDepartment(db))

	//fmt.Println(getAllEmployee(db))

	//fmt.Println("Total updated data", updateEmployeeName(db,"zioga",3))

	//fmt.Println("Total updated data", updateDepartmentName(db,"HRD",1))

	//fmt.Println("Total deleted date", deleteEmployee(db,3))

	//fmt.Println("Total deleted date", deleteDepartment(db,2))

	//Tugas 3
	//department := Department{"Finance"}
	//pkd := insertDepartment(db,department)
	//fmt.Printf("ID = %d \n", pkd)

	//pk := insertEmployee( db, Employee{"Fifeka",pkd})
	//fmt.Println("employee ID =", pk)

	//pk := insertEmployee( db, Employee{"Onanda",pkd})
	//fmt.Println("employee ID =", pk)

	//pk  := insertDepartment(db,Department{"Engineering"})
	//fmt.Println("Total Updated Data", updateEmployeeDepartment(db,pk,1))

	//Tugas 4

	batchEmployees := []Employee{}
	batchEmployees = append(batchEmployees, Employee{"Albert", 1})
	batchEmployees = append(batchEmployees, Employee{"Einstein", 1})

	batchInsertEmployee(db, batchEmployees)

	//batchInsertDepartment(db, []Department{{"accounting"},{"BOD"}})

}

func createDepartmentTable(db *sql.DB) {

	query := `CREATE TABLE IF NOT EXISTS Department (
			department_id SERIAL PRIMARY KEY,
			department_name VARCHAR(100) NOT NULL)`

	_, err := db.Exec(query)

	if err != nil {
		log.Fatalln(err)
	}
}

func createEmployeeTable(db *sql.DB) {
	query := `CREATE TABLE IF NOT EXISTS Employee (
			employee_id SERIAL PRIMARY KEY,
			employee_name VARCHAR(100) NOT NULL,
		  	department_id int,
		  	constraint fk_department
		  	foreign key (department_id)
		  	references department(department_id)
      		)`

	_, err := db.Exec(query)

	if err != nil {
		log.Fatalln(err)
	}
}

func insertDepartment(db *sql.DB, department Department) int {
	query := `INSERT INTO  Department (department_name) 
			VALUES ($1) RETURNING department_id`

	var pk int
	err := db.QueryRow(query, department.departmentName).Scan(&pk)
	if err != nil {
		log.Fatalln(err)
	}
	return pk
}

func insertEmployee(db *sql.DB, employee Employee) int {
	query := `INSERT INTO  Employee (employee_name, department_id) 
			VALUES ($1,$2) RETURNING employee_id`

	var pk int
	err := db.QueryRow(query, employee.employeeName, employee.departmentID).Scan(&pk)
	if err != nil {
		log.Fatalln(err)
	}
	return pk
}

func getAllDepartment(db *sql.DB) []Department {
	departments := []Department{}

	rows, err := db.Query("select department_name from department")
	if err != nil {
		log.Fatalln(err)
	}
	defer rows.Close()
	var departmentName string
	for rows.Next() {
		err := rows.Scan(&departmentName)
		if err != nil {
			log.Fatalln(err)
		}
		departments = append(departments, Department{departmentName})
	}
	return departments
}

func getAllEmployee(db *sql.DB) []Employee {
	employees := []Employee{}

	rows, err := db.Query("select employee_name, department_id from employee")
	if err != nil {
		log.Fatalln(err)
	}
	defer rows.Close()
	var employeeName string
	var departmentID int
	for rows.Next() {
		err := rows.Scan(&employeeName, &departmentID)
		if err != nil {
			log.Fatalln(err)
		}
		employees = append(employees, Employee{employeeName, departmentID})
	}
	return employees
}

func updateEmployeeName(db *sql.DB, employeeName string, employeeID int) int {
	query := `UPDATE employee
		SET employee_name = $1
		WHERE employee_id=$2`

	res, err := db.Exec(query, employeeName, employeeID)
	if err != nil {
		log.Fatalln(err)
	}
	count, err := res.RowsAffected()
	if err != nil {
		log.Fatalln(err)
	}
	return int(count)
}

func updateEmployeeDepartment(db *sql.DB, departmentID, employeeID int) int {
	query := `UPDATE employee
		SET department_id = $1
		WHERE employee_id=$2`

	res, err := db.Exec(query, departmentID, employeeID)
	if err != nil {
		log.Fatalln(err)
	}
	count, err := res.RowsAffected()
	if err != nil {
		log.Fatalln(err)
	}
	return int(count)
}

func updateDepartmentName(db *sql.DB, departmentName string, departmentID int) int {
	query := `UPDATE department
		SET department_name = $1
		WHERE department_id=$2`

	res, err := db.Exec(query, departmentName, departmentID)
	if err != nil {
		log.Fatalln(err)
	}
	count, err := res.RowsAffected()
	if err != nil {
		log.Fatalln(err)
	}
	return int(count)
}

func deleteEmployee(db *sql.DB, employeeID int) int {

	query := `DELETE FROM employee
		WHERE employee_id=$1`

	res, err := db.Exec(query, employeeID)
	if err != nil {
		log.Fatalln(err)
	}
	count, err := res.RowsAffected()
	if err != nil {
		log.Fatalln(err)
	}
	return int(count)

}

func deleteDepartment(db *sql.DB, departmentID int) int {
	query := `DELETE FROM department
		WHERE department_id=$1`

	res, err := db.Exec(query, departmentID)
	if err != nil {
		log.Fatalln(err)
	}
	count, err := res.RowsAffected()
	if err != nil {
		log.Fatalln(err)
	}
	return int(count)
}

func batchInsertEmployee(db *sql.DB, employees []Employee) {

	valueStrings := []string{}
	valueArgs := []interface{}{}

	counter := 1
	for _, employee := range employees {
		valueStrings = append(valueStrings, fmt.Sprintf("($%d,$%d)", counter, counter+1))
		valueArgs = append(valueArgs, employee.employeeName)
		valueArgs = append(valueArgs, employee.departmentID)
		counter = counter + 2
	}

	query := fmt.Sprintf(`INSERT INTO  Employee (employee_name, department_id) VALUES %s`, strings.Join(valueStrings, ","))
	tx, _ := db.Begin()
	_, err := tx.Exec(query, valueArgs...)

	if err != nil {
		tx.Rollback()
		log.Fatalln(err)
	}

	err = tx.Commit()
	if err != nil {
		fmt.Println(err)
	}
}

func batchInsertDepartment(db *sql.DB, departments []Department) {

	valueStrings := []string{}
	valueArgs := []interface{}{}

	counter := 1
	for _, department := range departments {
		valueStrings = append(valueStrings, fmt.Sprintf("($%d)", counter))
		valueArgs = append(valueArgs, department.departmentName)
		counter++
	}

	query := fmt.Sprintf(`INSERT INTO  department (department_name) VALUES %s`, strings.Join(valueStrings, ","))
	tx, _ := db.Begin()
	_, err := tx.Exec(query, valueArgs...)

	if err != nil {
		tx.Rollback()
		log.Fatalln(err)
	}

	err = tx.Commit()
	if err != nil {
		fmt.Println(err)
	}
}
