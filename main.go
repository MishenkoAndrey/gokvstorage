package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

func main() {
	startServer()
}

type transaction struct {
	keysToDelete   []string
	transactionMap map[string]*string
}

func startServer() {
	reader := bufio.NewReader(os.Stdin)
	db := map[string]*string{}
	var transactionsStack []transaction
	for {
		fmt.Print(">")
		args, _, _ := reader.ReadLine()
		a := strings.Split(string(args), " ")
		var action, arg string
		if len(a) < 2 {
			action = a[0]
		} else {
			action, arg = a[0], a[1]
		}
		switch action {
		case "SET":
			key, value := a[1], a[2]
			setValue(&key, &value, db, &transactionsStack)
		case "GET":
			res, ok := getValue(&arg, db, transactionsStack)
			if !ok {
				fmt.Println("NULL")
			} else {
				fmt.Println(*res)
			}
		case "UNSET":
			unsetValue(&arg, db, &transactionsStack)
		case "COUNT":
			fmt.Println(count(&arg, db, &transactionsStack))
		case "BEGIN":
			beginTransaction(&transactionsStack)
			fmt.Println("BEGIN")
		case "ROLLBACK":
			if rollbackLastTransaction(&transactionsStack) {
				fmt.Println("All changes after last BEGIN canceled.")
			} else {
				fmt.Println("No active transactions. Start new transaction with BEGIN.")
			}
		case "COMMIT":
			commitTransactions(&transactionsStack, db)
		case "END":
			fmt.Println("")
			break
		}
	}
}

func setValue(key *string, value *string, db map[string]*string, stack *[]transaction) {
	if len(*stack) >= 1 {
		(*stack)[len(*stack)-1].transactionMap[*key] = value
	} else {
		db[*key] = value
	}
}

func getValue(key *string, db map[string]*string, stack []transaction) (*string, bool) {
	lastTransaction := getLastTransaction(&stack)
	a := ""
	if checkItemInSlice(&lastTransaction.keysToDelete, key) {
		return &a, false
	}
	var value, ok = lastTransaction.transactionMap[*key]
	if !ok {
		value, ok = db[*key]
	}
	return value, ok
}

func unsetValue(key *string, db map[string]*string, stack *[]transaction) {
	if len(*stack) >= 1 {
		(*stack)[len(*stack)-1].keysToDelete = append((*stack)[len(*stack)-1].keysToDelete, *key)
		delete((*stack)[len(*stack)-1].transactionMap, *key)
	} else {
		delete(db, *key)
	}
}

func count(value *string, db map[string]*string, stack *[]transaction) int {
	res := 0
	var visitedKeys []string
	if len(*stack) >= 1 {
		for k, v := range (*stack)[len(*stack)-1].transactionMap {
			visitedKeys = append(visitedKeys, k)
			if *v == *value {
				res++
			}
		}
	}
	for k, v := range db {
		if checkItemInSlice(&visitedKeys, &k) {
			continue
		}
		if *v == *value {
			res++
		}
	}
	return res
}

func getLastTransaction(stack *[]transaction) transaction {
	if len(*stack) == 0 {
		return transaction{}
	}
	return (*stack)[len(*stack)-1]
}

func rollbackLastTransaction(stack *[]transaction) bool {
	if len(*stack) >= 1 {
		*stack = (*stack)[:len(*stack)-1]
		return true
	} else {
		return false
	}
}

func commitTransactions(stack *[]transaction, into map[string]*string) {
	for _, item := range *stack {
		for k, v := range item.transactionMap {
			into[k] = v
		}
	}
	*stack = []transaction{}
}

func beginTransaction(stack *[]transaction) {
	*stack = append(*stack, transaction{keysToDelete: []string{}, transactionMap: map[string]*string{}})
}

func checkItemInSlice(slice *[]string, item *string) bool {
	for _, elem := range *slice {
		if elem == *item {
			return true
		}
	}
	return false
}
