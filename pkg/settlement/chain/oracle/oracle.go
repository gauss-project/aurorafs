// Code generated - DO NOT EDIT.
// This file is a generated binding and any manual changes will be lost.

package oracle

import (
	"errors"
	"math/big"
	"strings"

	ethereum "github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/accounts/abi/bind"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/event"
)

// Reference imports to suppress errors if they are not otherwise used.
var (
	_ = errors.New
	_ = big.NewInt
	_ = strings.NewReader
	_ = ethereum.NotFound
	_ = bind.Bind
	_ = common.Big1
	_ = types.BloomLookup
	_ = event.NewSubscription
)

// OracleMetaData contains all meta data concerning the Oracle contract.
var OracleMetaData = &bind.MetaData{
	ABI: "[{\"inputs\":[{\"internalType\":\"bytes32\",\"name\":\"\",\"type\":\"bytes32\"}],\"name\":\"hashIMap\",\"outputs\":[{\"internalType\":\"uint256\",\"name\":\"\",\"type\":\"uint256\"}],\"stateMutability\":\"view\",\"type\":\"function\",\"constant\":true},{\"inputs\":[{\"internalType\":\"bytes32\",\"name\":\"\",\"type\":\"bytes32\"},{\"internalType\":\"bytes32\",\"name\":\"\",\"type\":\"bytes32\"}],\"name\":\"oracleIMap\",\"outputs\":[{\"internalType\":\"uint256\",\"name\":\"\",\"type\":\"uint256\"}],\"stateMutability\":\"view\",\"type\":\"function\",\"constant\":true},{\"inputs\":[],\"name\":\"owner\",\"outputs\":[{\"internalType\":\"address\",\"name\":\"\",\"type\":\"address\"}],\"stateMutability\":\"view\",\"type\":\"function\",\"constant\":true},{\"inputs\":[],\"name\":\"start\",\"outputs\":[{\"internalType\":\"bool\",\"name\":\"\",\"type\":\"bool\"}],\"stateMutability\":\"view\",\"type\":\"function\",\"constant\":true},{\"inputs\":[],\"name\":\"initialize\",\"outputs\":[],\"stateMutability\":\"nonpayable\",\"type\":\"function\"},{\"inputs\":[{\"internalType\":\"bool\",\"name\":\"value\",\"type\":\"bool\"}],\"name\":\"setStart\",\"outputs\":[],\"stateMutability\":\"nonpayable\",\"type\":\"function\"},{\"inputs\":[{\"internalType\":\"bytes32\",\"name\":\"hash\",\"type\":\"bytes32\"}],\"name\":\"get\",\"outputs\":[{\"internalType\":\"bytes32[]\",\"name\":\"\",\"type\":\"bytes32[]\"}],\"stateMutability\":\"view\",\"type\":\"function\",\"constant\":true},{\"inputs\":[{\"internalType\":\"bytes32\",\"name\":\"hash\",\"type\":\"bytes32\"},{\"internalType\":\"bytes32\",\"name\":\"addr\",\"type\":\"bytes32\"}],\"name\":\"set\",\"outputs\":[],\"stateMutability\":\"nonpayable\",\"type\":\"function\"},{\"inputs\":[{\"internalType\":\"bytes32\",\"name\":\"hash\",\"type\":\"bytes32\"},{\"internalType\":\"bytes32\",\"name\":\"addr\",\"type\":\"bytes32\"}],\"name\":\"remove\",\"outputs\":[],\"stateMutability\":\"nonpayable\",\"type\":\"function\"},{\"inputs\":[{\"internalType\":\"bytes32\",\"name\":\"hash\",\"type\":\"bytes32\"}],\"name\":\"clear\",\"outputs\":[],\"stateMutability\":\"nonpayable\",\"type\":\"function\"},{\"inputs\":[],\"name\":\"getFileList\",\"outputs\":[{\"internalType\":\"bytes32[]\",\"name\":\"\",\"type\":\"bytes32[]\"}],\"stateMutability\":\"view\",\"type\":\"function\",\"constant\":true}]",
}

// OracleABI is the input ABI used to generate the binding from.
// Deprecated: Use OracleMetaData.ABI instead.
var OracleABI = OracleMetaData.ABI

// Oracle is an auto generated Go binding around an Ethereum contract.
type Oracle struct {
	OracleCaller     // Read-only binding to the contract
	OracleTransactor // Write-only binding to the contract
	OracleFilterer   // Log filterer for contract events
}

// OracleCaller is an auto generated read-only Go binding around an Ethereum contract.
type OracleCaller struct {
	contract *bind.BoundContract // Generic contract wrapper for the low level calls
}

// OracleTransactor is an auto generated write-only Go binding around an Ethereum contract.
type OracleTransactor struct {
	contract *bind.BoundContract // Generic contract wrapper for the low level calls
}

// OracleFilterer is an auto generated log filtering Go binding around an Ethereum contract events.
type OracleFilterer struct {
	contract *bind.BoundContract // Generic contract wrapper for the low level calls
}

// OracleSession is an auto generated Go binding around an Ethereum contract,
// with pre-set call and transact options.
type OracleSession struct {
	Contract     *Oracle           // Generic contract binding to set the session for
	CallOpts     bind.CallOpts     // Call options to use throughout this session
	TransactOpts bind.TransactOpts // Transaction auth options to use throughout this session
}

// OracleCallerSession is an auto generated read-only Go binding around an Ethereum contract,
// with pre-set call options.
type OracleCallerSession struct {
	Contract *OracleCaller // Generic contract caller binding to set the session for
	CallOpts bind.CallOpts // Call options to use throughout this session
}

// OracleTransactorSession is an auto generated write-only Go binding around an Ethereum contract,
// with pre-set transact options.
type OracleTransactorSession struct {
	Contract     *OracleTransactor // Generic contract transactor binding to set the session for
	TransactOpts bind.TransactOpts // Transaction auth options to use throughout this session
}

// OracleRaw is an auto generated low-level Go binding around an Ethereum contract.
type OracleRaw struct {
	Contract *Oracle // Generic contract binding to access the raw methods on
}

// OracleCallerRaw is an auto generated low-level read-only Go binding around an Ethereum contract.
type OracleCallerRaw struct {
	Contract *OracleCaller // Generic read-only contract binding to access the raw methods on
}

// OracleTransactorRaw is an auto generated low-level write-only Go binding around an Ethereum contract.
type OracleTransactorRaw struct {
	Contract *OracleTransactor // Generic write-only contract binding to access the raw methods on
}

// NewOracle creates a new instance of Oracle, bound to a specific deployed contract.
func NewOracle(address common.Address, backend bind.ContractBackend) (*Oracle, error) {
	contract, err := bindOracle(address, backend, backend, backend)
	if err != nil {
		return nil, err
	}
	return &Oracle{OracleCaller: OracleCaller{contract: contract}, OracleTransactor: OracleTransactor{contract: contract}, OracleFilterer: OracleFilterer{contract: contract}}, nil
}

// NewOracleCaller creates a new read-only instance of Oracle, bound to a specific deployed contract.
func NewOracleCaller(address common.Address, caller bind.ContractCaller) (*OracleCaller, error) {
	contract, err := bindOracle(address, caller, nil, nil)
	if err != nil {
		return nil, err
	}
	return &OracleCaller{contract: contract}, nil
}

// NewOracleTransactor creates a new write-only instance of Oracle, bound to a specific deployed contract.
func NewOracleTransactor(address common.Address, transactor bind.ContractTransactor) (*OracleTransactor, error) {
	contract, err := bindOracle(address, nil, transactor, nil)
	if err != nil {
		return nil, err
	}
	return &OracleTransactor{contract: contract}, nil
}

// NewOracleFilterer creates a new log filterer instance of Oracle, bound to a specific deployed contract.
func NewOracleFilterer(address common.Address, filterer bind.ContractFilterer) (*OracleFilterer, error) {
	contract, err := bindOracle(address, nil, nil, filterer)
	if err != nil {
		return nil, err
	}
	return &OracleFilterer{contract: contract}, nil
}

// bindOracle binds a generic wrapper to an already deployed contract.
func bindOracle(address common.Address, caller bind.ContractCaller, transactor bind.ContractTransactor, filterer bind.ContractFilterer) (*bind.BoundContract, error) {
	parsed, err := abi.JSON(strings.NewReader(OracleABI))
	if err != nil {
		return nil, err
	}
	return bind.NewBoundContract(address, parsed, caller, transactor, filterer), nil
}

// Call invokes the (constant) contract method with params as input values and
// sets the output to result. The result type might be a single field for simple
// returns, a slice of interfaces for anonymous returns and a struct for named
// returns.
func (_Oracle *OracleRaw) Call(opts *bind.CallOpts, result *[]interface{}, method string, params ...interface{}) error {
	return _Oracle.Contract.OracleCaller.contract.Call(opts, result, method, params...)
}

// Transfer initiates a plain transaction to move funds to the contract, calling
// its default method if one is available.
func (_Oracle *OracleRaw) Transfer(opts *bind.TransactOpts) (*types.Transaction, error) {
	return _Oracle.Contract.OracleTransactor.contract.Transfer(opts)
}

// Transact invokes the (paid) contract method with params as input values.
func (_Oracle *OracleRaw) Transact(opts *bind.TransactOpts, method string, params ...interface{}) (*types.Transaction, error) {
	return _Oracle.Contract.OracleTransactor.contract.Transact(opts, method, params...)
}

// Call invokes the (constant) contract method with params as input values and
// sets the output to result. The result type might be a single field for simple
// returns, a slice of interfaces for anonymous returns and a struct for named
// returns.
func (_Oracle *OracleCallerRaw) Call(opts *bind.CallOpts, result *[]interface{}, method string, params ...interface{}) error {
	return _Oracle.Contract.contract.Call(opts, result, method, params...)
}

// Transfer initiates a plain transaction to move funds to the contract, calling
// its default method if one is available.
func (_Oracle *OracleTransactorRaw) Transfer(opts *bind.TransactOpts) (*types.Transaction, error) {
	return _Oracle.Contract.contract.Transfer(opts)
}

// Transact invokes the (paid) contract method with params as input values.
func (_Oracle *OracleTransactorRaw) Transact(opts *bind.TransactOpts, method string, params ...interface{}) (*types.Transaction, error) {
	return _Oracle.Contract.contract.Transact(opts, method, params...)
}

// Get is a free data retrieval call binding the contract method 0x8eaa6ac0.
//
// Solidity: function get(bytes32 hash) view returns(bytes32[])
func (_Oracle *OracleCaller) Get(opts *bind.CallOpts, hash [32]byte) ([][32]byte, error) {
	var out []interface{}
	err := _Oracle.contract.Call(opts, &out, "get", hash)

	if err != nil {
		return *new([][32]byte), err
	}

	out0 := *abi.ConvertType(out[0], new([][32]byte)).(*[][32]byte)

	return out0, err

}

// Get is a free data retrieval call binding the contract method 0x8eaa6ac0.
//
// Solidity: function get(bytes32 hash) view returns(bytes32[])
func (_Oracle *OracleSession) Get(hash [32]byte) ([][32]byte, error) {
	return _Oracle.Contract.Get(&_Oracle.CallOpts, hash)
}

// Get is a free data retrieval call binding the contract method 0x8eaa6ac0.
//
// Solidity: function get(bytes32 hash) view returns(bytes32[])
func (_Oracle *OracleCallerSession) Get(hash [32]byte) ([][32]byte, error) {
	return _Oracle.Contract.Get(&_Oracle.CallOpts, hash)
}

// GetFileList is a free data retrieval call binding the contract method 0x4cedee48.
//
// Solidity: function getFileList() view returns(bytes32[])
func (_Oracle *OracleCaller) GetFileList(opts *bind.CallOpts) ([][32]byte, error) {
	var out []interface{}
	err := _Oracle.contract.Call(opts, &out, "getFileList")

	if err != nil {
		return *new([][32]byte), err
	}

	out0 := *abi.ConvertType(out[0], new([][32]byte)).(*[][32]byte)

	return out0, err

}

// GetFileList is a free data retrieval call binding the contract method 0x4cedee48.
//
// Solidity: function getFileList() view returns(bytes32[])
func (_Oracle *OracleSession) GetFileList() ([][32]byte, error) {
	return _Oracle.Contract.GetFileList(&_Oracle.CallOpts)
}

// GetFileList is a free data retrieval call binding the contract method 0x4cedee48.
//
// Solidity: function getFileList() view returns(bytes32[])
func (_Oracle *OracleCallerSession) GetFileList() ([][32]byte, error) {
	return _Oracle.Contract.GetFileList(&_Oracle.CallOpts)
}

// HashIMap is a free data retrieval call binding the contract method 0x64f32492.
//
// Solidity: function hashIMap(bytes32 ) view returns(uint256)
func (_Oracle *OracleCaller) HashIMap(opts *bind.CallOpts, arg0 [32]byte) (*big.Int, error) {
	var out []interface{}
	err := _Oracle.contract.Call(opts, &out, "hashIMap", arg0)

	if err != nil {
		return *new(*big.Int), err
	}

	out0 := *abi.ConvertType(out[0], new(*big.Int)).(**big.Int)

	return out0, err

}

// HashIMap is a free data retrieval call binding the contract method 0x64f32492.
//
// Solidity: function hashIMap(bytes32 ) view returns(uint256)
func (_Oracle *OracleSession) HashIMap(arg0 [32]byte) (*big.Int, error) {
	return _Oracle.Contract.HashIMap(&_Oracle.CallOpts, arg0)
}

// HashIMap is a free data retrieval call binding the contract method 0x64f32492.
//
// Solidity: function hashIMap(bytes32 ) view returns(uint256)
func (_Oracle *OracleCallerSession) HashIMap(arg0 [32]byte) (*big.Int, error) {
	return _Oracle.Contract.HashIMap(&_Oracle.CallOpts, arg0)
}

// OracleIMap is a free data retrieval call binding the contract method 0xce9972f9.
//
// Solidity: function oracleIMap(bytes32 , bytes32 ) view returns(uint256)
func (_Oracle *OracleCaller) OracleIMap(opts *bind.CallOpts, arg0 [32]byte, arg1 [32]byte) (*big.Int, error) {
	var out []interface{}
	err := _Oracle.contract.Call(opts, &out, "oracleIMap", arg0, arg1)

	if err != nil {
		return *new(*big.Int), err
	}

	out0 := *abi.ConvertType(out[0], new(*big.Int)).(**big.Int)

	return out0, err

}

// OracleIMap is a free data retrieval call binding the contract method 0xce9972f9.
//
// Solidity: function oracleIMap(bytes32 , bytes32 ) view returns(uint256)
func (_Oracle *OracleSession) OracleIMap(arg0 [32]byte, arg1 [32]byte) (*big.Int, error) {
	return _Oracle.Contract.OracleIMap(&_Oracle.CallOpts, arg0, arg1)
}

// OracleIMap is a free data retrieval call binding the contract method 0xce9972f9.
//
// Solidity: function oracleIMap(bytes32 , bytes32 ) view returns(uint256)
func (_Oracle *OracleCallerSession) OracleIMap(arg0 [32]byte, arg1 [32]byte) (*big.Int, error) {
	return _Oracle.Contract.OracleIMap(&_Oracle.CallOpts, arg0, arg1)
}

// Owner is a free data retrieval call binding the contract method 0x8da5cb5b.
//
// Solidity: function owner() view returns(address)
func (_Oracle *OracleCaller) Owner(opts *bind.CallOpts) (common.Address, error) {
	var out []interface{}
	err := _Oracle.contract.Call(opts, &out, "owner")

	if err != nil {
		return *new(common.Address), err
	}

	out0 := *abi.ConvertType(out[0], new(common.Address)).(*common.Address)

	return out0, err

}

// Owner is a free data retrieval call binding the contract method 0x8da5cb5b.
//
// Solidity: function owner() view returns(address)
func (_Oracle *OracleSession) Owner() (common.Address, error) {
	return _Oracle.Contract.Owner(&_Oracle.CallOpts)
}

// Owner is a free data retrieval call binding the contract method 0x8da5cb5b.
//
// Solidity: function owner() view returns(address)
func (_Oracle *OracleCallerSession) Owner() (common.Address, error) {
	return _Oracle.Contract.Owner(&_Oracle.CallOpts)
}

// Start is a free data retrieval call binding the contract method 0xbe9a6555.
//
// Solidity: function start() view returns(bool)
func (_Oracle *OracleCaller) Start(opts *bind.CallOpts) (bool, error) {
	var out []interface{}
	err := _Oracle.contract.Call(opts, &out, "start")

	if err != nil {
		return *new(bool), err
	}

	out0 := *abi.ConvertType(out[0], new(bool)).(*bool)

	return out0, err

}

// Start is a free data retrieval call binding the contract method 0xbe9a6555.
//
// Solidity: function start() view returns(bool)
func (_Oracle *OracleSession) Start() (bool, error) {
	return _Oracle.Contract.Start(&_Oracle.CallOpts)
}

// Start is a free data retrieval call binding the contract method 0xbe9a6555.
//
// Solidity: function start() view returns(bool)
func (_Oracle *OracleCallerSession) Start() (bool, error) {
	return _Oracle.Contract.Start(&_Oracle.CallOpts)
}

// Clear is a paid mutator transaction binding the contract method 0x97040a45.
//
// Solidity: function clear(bytes32 hash) returns()
func (_Oracle *OracleTransactor) Clear(opts *bind.TransactOpts, hash [32]byte) (*types.Transaction, error) {
	return _Oracle.contract.Transact(opts, "clear", hash)
}

// Clear is a paid mutator transaction binding the contract method 0x97040a45.
//
// Solidity: function clear(bytes32 hash) returns()
func (_Oracle *OracleSession) Clear(hash [32]byte) (*types.Transaction, error) {
	return _Oracle.Contract.Clear(&_Oracle.TransactOpts, hash)
}

// Clear is a paid mutator transaction binding the contract method 0x97040a45.
//
// Solidity: function clear(bytes32 hash) returns()
func (_Oracle *OracleTransactorSession) Clear(hash [32]byte) (*types.Transaction, error) {
	return _Oracle.Contract.Clear(&_Oracle.TransactOpts, hash)
}

// Initialize is a paid mutator transaction binding the contract method 0x8129fc1c.
//
// Solidity: function initialize() returns()
func (_Oracle *OracleTransactor) Initialize(opts *bind.TransactOpts) (*types.Transaction, error) {
	return _Oracle.contract.Transact(opts, "initialize")
}

// Initialize is a paid mutator transaction binding the contract method 0x8129fc1c.
//
// Solidity: function initialize() returns()
func (_Oracle *OracleSession) Initialize() (*types.Transaction, error) {
	return _Oracle.Contract.Initialize(&_Oracle.TransactOpts)
}

// Initialize is a paid mutator transaction binding the contract method 0x8129fc1c.
//
// Solidity: function initialize() returns()
func (_Oracle *OracleTransactorSession) Initialize() (*types.Transaction, error) {
	return _Oracle.Contract.Initialize(&_Oracle.TransactOpts)
}

// Remove is a paid mutator transaction binding the contract method 0xb10e4172.
//
// Solidity: function remove(bytes32 hash, bytes32 addr) returns()
func (_Oracle *OracleTransactor) Remove(opts *bind.TransactOpts, hash [32]byte, addr [32]byte) (*types.Transaction, error) {
	return _Oracle.contract.Transact(opts, "remove", hash, addr)
}

// Remove is a paid mutator transaction binding the contract method 0xb10e4172.
//
// Solidity: function remove(bytes32 hash, bytes32 addr) returns()
func (_Oracle *OracleSession) Remove(hash [32]byte, addr [32]byte) (*types.Transaction, error) {
	return _Oracle.Contract.Remove(&_Oracle.TransactOpts, hash, addr)
}

// Remove is a paid mutator transaction binding the contract method 0xb10e4172.
//
// Solidity: function remove(bytes32 hash, bytes32 addr) returns()
func (_Oracle *OracleTransactorSession) Remove(hash [32]byte, addr [32]byte) (*types.Transaction, error) {
	return _Oracle.Contract.Remove(&_Oracle.TransactOpts, hash, addr)
}

// Set is a paid mutator transaction binding the contract method 0xf71f7a25.
//
// Solidity: function set(bytes32 hash, bytes32 addr) returns()
func (_Oracle *OracleTransactor) Set(opts *bind.TransactOpts, hash [32]byte, addr [32]byte) (*types.Transaction, error) {
	return _Oracle.contract.Transact(opts, "set", hash, addr)
}

// Set is a paid mutator transaction binding the contract method 0xf71f7a25.
//
// Solidity: function set(bytes32 hash, bytes32 addr) returns()
func (_Oracle *OracleSession) Set(hash [32]byte, addr [32]byte) (*types.Transaction, error) {
	return _Oracle.Contract.Set(&_Oracle.TransactOpts, hash, addr)
}

// Set is a paid mutator transaction binding the contract method 0xf71f7a25.
//
// Solidity: function set(bytes32 hash, bytes32 addr) returns()
func (_Oracle *OracleTransactorSession) Set(hash [32]byte, addr [32]byte) (*types.Transaction, error) {
	return _Oracle.Contract.Set(&_Oracle.TransactOpts, hash, addr)
}

// SetStart is a paid mutator transaction binding the contract method 0x68e24327.
//
// Solidity: function setStart(bool value) returns()
func (_Oracle *OracleTransactor) SetStart(opts *bind.TransactOpts, value bool) (*types.Transaction, error) {
	return _Oracle.contract.Transact(opts, "setStart", value)
}

// SetStart is a paid mutator transaction binding the contract method 0x68e24327.
//
// Solidity: function setStart(bool value) returns()
func (_Oracle *OracleSession) SetStart(value bool) (*types.Transaction, error) {
	return _Oracle.Contract.SetStart(&_Oracle.TransactOpts, value)
}

// SetStart is a paid mutator transaction binding the contract method 0x68e24327.
//
// Solidity: function setStart(bool value) returns()
func (_Oracle *OracleTransactorSession) SetStart(value bool) (*types.Transaction, error) {
	return _Oracle.Contract.SetStart(&_Oracle.TransactOpts, value)
}
