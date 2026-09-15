package main

import (
	"fmt"

	"github.com/alex-njaiya/popeye_the_sailor/cmd/simulate"
)

func main() {
	fmt.Println("This is the entry point of the app")

	cfg := simulate.SimConfig{
		HouseEdge:        0.03,
		StartingBankroll: 100000,
		NumRounds:        10000,
		AvgBetSize:       10.0,
		BetSizeStdDev:    0.8,
		AvgCashoutTarget: 2.0,
		CashoutStdDev:    0.5,
		PlayersPerRound:  20,
	}

	simResults := simulate.RunSimulation(cfg)

	result := simulate.SimResult{
		FinalBankroll:   simResults.FinalBankroll,
		MinBankroll:     simResults.MinBankroll,
		MaxDrawdown:     simResults.MaxDrawdown,
		BankrollHistory: simResults.BankrollHistory,
	}

	fmt.Printf(
		"final bank roll: %d\n"+
			"min bankroll: %d\n"+
			"max drawdown: %d\n"+
			"bank history: [%d]\n",
		result.FinalBankroll, result.MinBankroll, result.MaxDrawdown, result.BankrollHistory,
	)
}
