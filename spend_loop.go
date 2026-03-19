package main

import (
	"encoding/hex"
	"fmt"
	"math/rand"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Hoosat-Oy/HTND/domain/consensus/utils/consensushashing"

	"github.com/Hoosat-Oy/HTND/domain/consensus/model/externalapi"
	"github.com/Hoosat-Oy/HTND/domain/consensus/utils/constants"
	"github.com/Hoosat-Oy/HTND/domain/consensus/utils/subnetworks"
	"github.com/Hoosat-Oy/HTND/domain/consensus/utils/transactionid"
	"github.com/Hoosat-Oy/HTND/domain/consensus/utils/txscript"
	"github.com/Hoosat-Oy/HTND/infrastructure/network/rpcclient"

	"github.com/Hoosat-Oy/HTND/app/appmessage"
	"github.com/Hoosat-Oy/HTND/util"
	"github.com/kaspanet/go-secp256k1"

	utxopkg "github.com/Hoosat-Oy/HTND/domain/consensus/utils/utxo"
	"github.com/pkg/errors"
)

var (
	pendingOutpoints      = make(map[appmessage.RPCOutpoint]time.Time)
	pendingOutpointsMutex sync.Mutex
)

func spendLoop(client *rpcclient.RPCClient, addresses *addressesList,
	utxosChangedNotificationChan <-chan *appmessage.UTXOsChangedNotificationMessage) <-chan struct{} {

	doneChan := make(chan struct{})

	spawn("spendLoop", func() {
		log.Infof("Fetching the initial UTXO set")
		utxos, err := fetchSpendableUTXOs(client, addresses.myAddress.EncodeAddress())
		if err != nil {
			panic(err)
		}
		log.Infof("Initial UTXO count %d\n", len(utxos))

		cfg := activeConfig()
		ticker := time.NewTicker(time.Duration(cfg.TransactionInterval) * time.Millisecond)
		for range ticker.C {
			healthChan := make(chan struct{})
			go func() {
				timer := time.NewTimer(1 * time.Minute)
				defer timer.Stop()
				select {
				case <-healthChan:
				case <-timer.C:
					log.Criticalf("HEALTCHECK FAILED")
					fmt.Println("HEALTCHECK FAILED")
					os.Exit(1)
				}
			}()
			// fmt.Printf("UTXOs: %d\n", len(utxos))
			hasFunds, err := maybeSendTransaction(client, addresses, utxos)
			if err != nil {
				panic(err)
			}

			checkTransactions(utxosChangedNotificationChan)

			if !hasFunds {
				log.Infof("No spendable UTXOs. Refetching UTXO set.")

				for {
					var err error
					utxos, err = fetchSpendableUTXOs(client, addresses.myAddress.EncodeAddress())

					if err == nil {
						log.Infof("New Spendable UTXO count %d", len(utxos))
						break
					}

					log.Warnf("Failed to fetch UTXOs: %v. Retrying in 2s...", err)
					time.Sleep(2 * time.Second)
				}
			}

			if atomic.LoadInt32(&shutdown) != 0 {
				close(doneChan)
				return
			}

			close(healthChan)
		}
	})

	return doneChan
}

func checkTransactions(utxosChangedNotificationChan <-chan *appmessage.UTXOsChangedNotificationMessage) {
	isDone := false
	for !isDone {
		select {
		case notification := <-utxosChangedNotificationChan:
			pendingOutpointsMutex.Lock()
			for _, removed := range notification.Removed {
				sendTime, ok := pendingOutpoints[*removed.Outpoint]
				if ok {
					log.Infof("Output %s:%d accepted. Time since send: %s",
						removed.Outpoint.TransactionID, removed.Outpoint.Index, time.Since(sendTime))
					delete(pendingOutpoints, *removed.Outpoint)
				}
			}
			pendingOutpointsMutex.Unlock()
		default:
			isDone = true
		}
	}

	pendingOutpointsMutex.Lock()
	defer pendingOutpointsMutex.Unlock()
	for pendingOutpoint, txTime := range pendingOutpoints {
		timeSince := time.Since(txTime)
		if timeSince > 10*time.Minute {
			log.Tracef("Outpoint %s:%d is pending for %s",
				pendingOutpoint.TransactionID, pendingOutpoint.Index, timeSince)
		}
	}
}

const balanceEpsilon = 10_000         // 10,000 sompi = 0.0001 Hoosat
const feeAmount = balanceEpsilon * 10 // use high fee amount, because can have a large number of inputs

var stats struct {
	sync.Mutex
	numTxs uint64
	since  time.Time
}

func maybeSendTransaction(client *rpcclient.RPCClient, addresses *addressesList,
	availableUTXOs map[appmessage.RPCOutpoint]*appmessage.RPCUTXOEntry) (hasFunds bool, err error) {

	cfg := activeConfig()
	var selectedUTXOs []*appmessage.UTXOsByAddressesEntry
	var selectedValue uint64

	if cfg.SingleOutput {
		selectedUTXOs, selectedValue, err = selectSingleUTXO(availableUTXOs, feeAmount+1)
		if err != nil {
			return false, err
		}
		if len(selectedUTXOs) == 0 {
			return false, nil
		}
	} else {
		const minChange = 100
		selectedUTXOs, selectedValue, err = selectSingleUTXO(availableUTXOs, feeAmount+minChange)
		if err != nil {
			return false, err
		}

		if len(selectedUTXOs) == 0 {
			return false, nil
		}
	}

	var change uint64
	sendAmount := uint64(0)
	if cfg.SingleOutput {
		sendAmount = selectedValue - feeAmount
		change = 0
	} else {
		const minChange = 1000
		sendAmount = selectedValue - feeAmount - minChange
		change = minChange
	}

	spendAddress := randomizeSpendAddress(addresses)

	rpcTransaction, err := generateTransaction(
		addresses.myPrivateKey, selectedUTXOs, sendAmount, change, spendAddress, addresses.myAddress)
	if err != nil {
		return false, err
	}

	if rpcTransaction.Outputs[0].Amount == 0 {
		log.Warnf("Got transaction with 0 value output")
		return false, nil
	}

	setPending(availableUTXOs, selectedUTXOs)
	spawn("sendTransaction", func() {
		transactionID, err := sendTransaction(client, rpcTransaction)
		if err != nil {
			errMessage := err.Error()
			if !strings.Contains(errMessage, "orphan transaction") &&
				!strings.Contains(errMessage, "is already in the mempool") &&
				!strings.Contains(errMessage, "is an orphan") &&
				!strings.Contains(errMessage, "already spent by transaction") {
				panic(errors.Wrapf(err, "error sending transaction: %s", err))
			}
		} else {
			log.Infof("Sent transaction %s worth %f hoosat with %d inputs and %d outputs", transactionID,
				float64(sendAmount)/constants.SompiPerHoosat, len(rpcTransaction.Inputs), len(rpcTransaction.Outputs))
			unsetPending(availableUTXOs, selectedUTXOs)
			func() {
				stats.Lock()
				defer stats.Unlock()

				stats.numTxs++
				timePast := time.Since(stats.since)
				if timePast > 10*time.Second {
					log.Infof("Tx rate: %f/sec", float64(stats.numTxs)/timePast.Seconds())
					stats.numTxs = 0
					stats.since = time.Now()
				}
			}()
		}
	})

	return true, nil
}

func fetchSpendableUTXOs(client *rpcclient.RPCClient, address string) (map[appmessage.RPCOutpoint]*appmessage.RPCUTXOEntry, error) {
	// Clean the pending.
	pendingOutpointsMutex.Lock()
	for k := range pendingOutpoints {
		delete(pendingOutpoints, k)
	}
	log.Infof("Cleared the pending Outpoints")
	pendingOutpointsMutex.Unlock()
	getUTXOsByAddressesResponse, err := client.GetUTXOsByAddresses([]string{address}, 1000)
	if err != nil {
		return nil, err
	}
	dagInfo, err := client.GetBlockDAGInfo()
	if err != nil {
		return nil, err
	}
	log.Infof("Checking if UTXO is spendable")
	spendableUTXOs := make(map[appmessage.RPCOutpoint]*appmessage.RPCUTXOEntry, 0)
	for _, entry := range getUTXOsByAddressesResponse.Entries {
		if !isUTXOSpendable(entry, dagInfo.VirtualDAAScore) {
			continue
		}
		spendableUTXOs[*entry.Outpoint] = entry.UTXOEntry
	}
	return spendableUTXOs, nil
}

func isUTXOSpendable(entry *appmessage.UTXOsByAddressesEntry, virtualSelectedParentBlueScore uint64) bool {
	blockDAAScore := entry.UTXOEntry.BlockDAAScore
	if !entry.UTXOEntry.IsCoinbase {
		const minConfirmations = 10
		return blockDAAScore+minConfirmations < virtualSelectedParentBlueScore
	}
	coinbaseMaturity := activeConfig().ActiveNetParams.BlockCoinbaseMaturity
	return blockDAAScore+coinbaseMaturity < virtualSelectedParentBlueScore
}

func setPending(availableUTXOs map[appmessage.RPCOutpoint]*appmessage.RPCUTXOEntry,
	selectedUTXOs []*appmessage.UTXOsByAddressesEntry) {
	pendingOutpointsMutex.Lock()
	defer pendingOutpointsMutex.Unlock()
	for _, utxo := range selectedUTXOs {
		pendingOutpoints[*utxo.Outpoint] = time.Now()
	}
}

func unsetPending(availableUTXOs map[appmessage.RPCOutpoint]*appmessage.RPCUTXOEntry,
	selectedUTXOs []*appmessage.UTXOsByAddressesEntry) {
	pendingOutpointsMutex.Lock()
	defer pendingOutpointsMutex.Unlock()
	for _, utxo := range selectedUTXOs {
		delete(pendingOutpoints, *utxo.Outpoint)
	}
}

func filterSpentUTXOsAndCalculateBalance(utxos []*appmessage.UTXOsByAddressesEntry) (
	filteredUTXOs []*appmessage.UTXOsByAddressesEntry, balance uint64) {

	balance = 0
	for _, utxo := range utxos {
		if _, ok := pendingOutpoints[*utxo.Outpoint]; ok {
			continue
		}
		balance += utxo.UTXOEntry.Amount
		filteredUTXOs = append(filteredUTXOs, utxo)
	}
	return filteredUTXOs, balance
}

func randomizeSpendAddress(addresses *addressesList) util.Address {
	spendAddressIndex := rand.Intn(len(addresses.spendAddresses))

	return addresses.spendAddresses[spendAddressIndex]
}

func randomizeSpendAmount() uint64 {
	const maxAmountToSent = 10 * feeAmount
	amountToSend := rand.Int63n(int64(maxAmountToSent))

	// round to balanceEpsilon
	amountToSend = amountToSend / balanceEpsilon * balanceEpsilon
	if amountToSend < balanceEpsilon {
		amountToSend = balanceEpsilon
	}

	return uint64(amountToSend)
}

func selectUTXOs(
	utxos map[appmessage.RPCOutpoint]*appmessage.RPCUTXOEntry,
	amountToSend uint64,
) (
	selectedUTXOs []*appmessage.UTXOsByAddressesEntry,
	selectedValue uint64,
	err error,
) {
	const maxInputs = 100

	selectedUTXOs = make([]*appmessage.UTXOsByAddressesEntry, 0, maxInputs)
	selectedValue = 0

	pendingOutpointsMutex.Lock()
	defer pendingOutpointsMutex.Unlock()

	for outpoint, entry := range utxos {
		if _, isPending := pendingOutpoints[outpoint]; isPending {
			continue
		}

		outpointCopy := outpoint
		selectedUTXOs = append(selectedUTXOs, &appmessage.UTXOsByAddressesEntry{
			Outpoint:  &outpointCopy,
			UTXOEntry: entry,
		})
		selectedValue += entry.Amount

		if selectedValue >= amountToSend {
			break
		}

		if len(selectedUTXOs) == maxInputs {
			log.Infof("Selected %d UTXOs so sending the transaction with %d sompis instead of %d", maxInputs, selectedValue, amountToSend)
			break
		}
	}

	return selectedUTXOs, selectedValue, nil
}

func selectSingleUTXO(
	utxos map[appmessage.RPCOutpoint]*appmessage.RPCUTXOEntry,
	minAmount uint64,
) (
	selectedUTXOs []*appmessage.UTXOsByAddressesEntry,
	selectedValue uint64,
	err error,
) {
	pendingOutpointsMutex.Lock()
	defer pendingOutpointsMutex.Unlock()

	maxAmount := uint64(0)
	for outpoint, entry := range utxos {
		if entry.Amount > maxAmount {
			maxAmount = entry.Amount
		}
		if _, isPending := pendingOutpoints[outpoint]; isPending {
			continue
		}

		if entry.Amount >= minAmount {
			outpointCopy := outpoint
			selectedUTXOs = []*appmessage.UTXOsByAddressesEntry{
				{
					Outpoint:  &outpointCopy,
					UTXOEntry: entry,
				},
			}
			selectedValue = entry.Amount
			break
		}
	}

	if len(selectedUTXOs) == 0 {
		log.Infof("No UTXO found with amount >= %d sompi. Max available: %d sompi", minAmount, maxAmount)
	}

	return selectedUTXOs, selectedValue, nil
}

func generateTransaction(keyPair *secp256k1.SchnorrKeyPair, selectedUTXOs []*appmessage.UTXOsByAddressesEntry,
	sompisToSend uint64, change uint64, toAddress util.Address,
	fromAddress util.Address) (*appmessage.RPCTransaction, error) {

	inputs := make([]*externalapi.DomainTransactionInput, len(selectedUTXOs))
	for i, utxo := range selectedUTXOs {
		outpointTransactionIDBytes, err := hex.DecodeString(utxo.Outpoint.TransactionID)
		if err != nil {
			return nil, err
		}
		outpointTransactionID, err := transactionid.FromBytes(outpointTransactionIDBytes)
		if err != nil {
			return nil, err
		}
		outpoint := externalapi.DomainOutpoint{
			TransactionID: *outpointTransactionID,
			Index:         utxo.Outpoint.Index,
		}
		utxoScriptPublicKeyScript, err := hex.DecodeString(utxo.UTXOEntry.ScriptPublicKey.Script)
		if err != nil {
			return nil, err
		}

		inputs[i] = &externalapi.DomainTransactionInput{
			PreviousOutpoint: outpoint,
			SigOpCount:       1,
			UTXOEntry: utxopkg.NewUTXOEntry(
				utxo.UTXOEntry.Amount,
				&externalapi.ScriptPublicKey{
					Script:  utxoScriptPublicKeyScript,
					Version: utxo.UTXOEntry.ScriptPublicKey.Version,
				},
				utxo.UTXOEntry.IsCoinbase,
				utxo.UTXOEntry.BlockDAAScore,
			),
		}
	}

	toScript, err := txscript.PayToAddrScript(toAddress)
	if err != nil {
		return nil, err
	}
	mainOutput := &externalapi.DomainTransactionOutput{
		Value:           sompisToSend,
		ScriptPublicKey: toScript,
	}
	fromScript, err := txscript.PayToAddrScript(fromAddress)
	if err != nil {
		return nil, err
	}
	outputs := []*externalapi.DomainTransactionOutput{mainOutput}
	if change > 0 {
		changeOutput := &externalapi.DomainTransactionOutput{
			Value:           change,
			ScriptPublicKey: fromScript,
		}
		outputs = append(outputs, changeOutput)
	}

	domainTransaction := &externalapi.DomainTransaction{
		Version:      constants.MaxTransactionVersion,
		Inputs:       inputs,
		Outputs:      outputs,
		LockTime:     0,
		SubnetworkID: subnetworks.SubnetworkIDNative,
		Gas:          0,
		Payload:      nil,
	}

	for i, input := range domainTransaction.Inputs {
		signatureScript, err := txscript.SignatureScript(domainTransaction, i, consensushashing.SigHashAll, keyPair,
			&consensushashing.SighashReusedValues{})
		if err != nil {
			return nil, err
		}
		input.SignatureScript = signatureScript
	}

	rpcTransaction := appmessage.DomainTransactionToRPCTransaction(domainTransaction)
	return rpcTransaction, nil
}

func sendTransaction(client *rpcclient.RPCClient, rpcTransaction *appmessage.RPCTransaction) (string, error) {
	tx, err := appmessage.RPCTransactionToDomainTransaction(rpcTransaction)
	if err != nil {
		return "", errors.Wrapf(err, "error submitting transaction")
	}
	submitTransactionResponse, err := client.SubmitTransaction(rpcTransaction, consensushashing.TransactionID(tx).String(), false)
	if err != nil {
		return "", errors.Wrapf(err, "error submitting transaction")
	}
	return submitTransactionResponse.TransactionID, nil
}

func init() {
	rand.Seed(time.Now().UnixNano())
}
