// Code generated - DO NOT EDIT.
// This file is a generated binding and any manual changes will be lost.

package traffic

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

// TrafficMetaData contains all meta data concerning the Traffic contract.
var TrafficMetaData = &bind.MetaData{
	ABI: "[{\"anonymous\":false,\"inputs\":[{\"indexed\":true,\"internalType\":\"address\",\"name\":\"owner\",\"type\":\"address\"},{\"indexed\":true,\"internalType\":\"address\",\"name\":\"spender\",\"type\":\"address\"},{\"indexed\":false,\"internalType\":\"uint256\",\"name\":\"value\",\"type\":\"uint256\"}],\"name\":\"Approval\",\"type\":\"event\"},{\"anonymous\":false,\"inputs\":[{\"indexed\":true,\"internalType\":\"address\",\"name\":\"from\",\"type\":\"address\"},{\"indexed\":true,\"internalType\":\"address\",\"name\":\"to\",\"type\":\"address\"},{\"indexed\":false,\"internalType\":\"uint256\",\"name\":\"value\",\"type\":\"uint256\"}],\"name\":\"Transfer\",\"type\":\"event\"},{\"inputs\":[],\"name\":\"CHEQUE_TYPEHASH\",\"outputs\":[{\"internalType\":\"bytes32\",\"name\":\"\",\"type\":\"bytes32\"}],\"stateMutability\":\"view\",\"type\":\"function\",\"constant\":true},{\"inputs\":[],\"name\":\"EIP712DOMAIN_TYPEHASH\",\"outputs\":[{\"internalType\":\"bytes32\",\"name\":\"\",\"type\":\"bytes32\"}],\"stateMutability\":\"view\",\"type\":\"function\",\"constant\":true},{\"inputs\":[{\"internalType\":\"address\",\"name\":\"owner\",\"type\":\"address\"},{\"internalType\":\"address\",\"name\":\"spender\",\"type\":\"address\"}],\"name\":\"allowance\",\"outputs\":[{\"internalType\":\"uint256\",\"name\":\"\",\"type\":\"uint256\"}],\"stateMutability\":\"view\",\"type\":\"function\",\"constant\":true},{\"inputs\":[{\"internalType\":\"address\",\"name\":\"spender\",\"type\":\"address\"},{\"internalType\":\"uint256\",\"name\":\"amount\",\"type\":\"uint256\"}],\"name\":\"approve\",\"outputs\":[{\"internalType\":\"bool\",\"name\":\"\",\"type\":\"bool\"}],\"stateMutability\":\"nonpayable\",\"type\":\"function\"},{\"inputs\":[{\"internalType\":\"address\",\"name\":\"account\",\"type\":\"address\"}],\"name\":\"balanceOf\",\"outputs\":[{\"internalType\":\"uint256\",\"name\":\"\",\"type\":\"uint256\"}],\"stateMutability\":\"view\",\"type\":\"function\",\"constant\":true},{\"inputs\":[],\"name\":\"decimals\",\"outputs\":[{\"internalType\":\"uint8\",\"name\":\"\",\"type\":\"uint8\"}],\"stateMutability\":\"view\",\"type\":\"function\",\"constant\":true},{\"inputs\":[{\"internalType\":\"address\",\"name\":\"spender\",\"type\":\"address\"},{\"internalType\":\"uint256\",\"name\":\"subtractedValue\",\"type\":\"uint256\"}],\"name\":\"decreaseAllowance\",\"outputs\":[{\"internalType\":\"bool\",\"name\":\"\",\"type\":\"bool\"}],\"stateMutability\":\"nonpayable\",\"type\":\"function\"},{\"inputs\":[{\"internalType\":\"address\",\"name\":\"spender\",\"type\":\"address\"},{\"internalType\":\"uint256\",\"name\":\"addedValue\",\"type\":\"uint256\"}],\"name\":\"increaseAllowance\",\"outputs\":[{\"internalType\":\"bool\",\"name\":\"\",\"type\":\"bool\"}],\"stateMutability\":\"nonpayable\",\"type\":\"function\"},{\"inputs\":[],\"name\":\"name\",\"outputs\":[{\"internalType\":\"string\",\"name\":\"\",\"type\":\"string\"}],\"stateMutability\":\"view\",\"type\":\"function\",\"constant\":true},{\"inputs\":[],\"name\":\"owner\",\"outputs\":[{\"internalType\":\"address\",\"name\":\"\",\"type\":\"address\"}],\"stateMutability\":\"view\",\"type\":\"function\",\"constant\":true},{\"inputs\":[{\"internalType\":\"address\",\"name\":\"\",\"type\":\"address\"},{\"internalType\":\"uint256\",\"name\":\"\",\"type\":\"uint256\"}],\"name\":\"retrievedAddress\",\"outputs\":[{\"internalType\":\"address\",\"name\":\"\",\"type\":\"address\"}],\"stateMutability\":\"view\",\"type\":\"function\",\"constant\":true},{\"inputs\":[{\"internalType\":\"address\",\"name\":\"\",\"type\":\"address\"}],\"name\":\"retrievedTotal\",\"outputs\":[{\"internalType\":\"uint256\",\"name\":\"\",\"type\":\"uint256\"}],\"stateMutability\":\"view\",\"type\":\"function\",\"constant\":true},{\"inputs\":[],\"name\":\"symbol\",\"outputs\":[{\"internalType\":\"string\",\"name\":\"\",\"type\":\"string\"}],\"stateMutability\":\"view\",\"type\":\"function\",\"constant\":true},{\"inputs\":[],\"name\":\"totalSupply\",\"outputs\":[{\"internalType\":\"uint256\",\"name\":\"\",\"type\":\"uint256\"}],\"stateMutability\":\"view\",\"type\":\"function\",\"constant\":true},{\"inputs\":[{\"internalType\":\"address\",\"name\":\"\",\"type\":\"address\"},{\"internalType\":\"address\",\"name\":\"\",\"type\":\"address\"}],\"name\":\"transTraffic\",\"outputs\":[{\"internalType\":\"uint256\",\"name\":\"\",\"type\":\"uint256\"}],\"stateMutability\":\"view\",\"type\":\"function\",\"constant\":true},{\"inputs\":[{\"internalType\":\"address\",\"name\":\"recipient\",\"type\":\"address\"},{\"internalType\":\"uint256\",\"name\":\"amount\",\"type\":\"uint256\"}],\"name\":\"transfer\",\"outputs\":[{\"internalType\":\"bool\",\"name\":\"\",\"type\":\"bool\"}],\"stateMutability\":\"nonpayable\",\"type\":\"function\"},{\"inputs\":[{\"internalType\":\"address\",\"name\":\"sender\",\"type\":\"address\"},{\"internalType\":\"address\",\"name\":\"recipient\",\"type\":\"address\"},{\"internalType\":\"uint256\",\"name\":\"amount\",\"type\":\"uint256\"}],\"name\":\"transferFrom\",\"outputs\":[{\"internalType\":\"bool\",\"name\":\"\",\"type\":\"bool\"}],\"stateMutability\":\"nonpayable\",\"type\":\"function\"},{\"inputs\":[{\"internalType\":\"address\",\"name\":\"\",\"type\":\"address\"},{\"internalType\":\"uint256\",\"name\":\"\",\"type\":\"uint256\"}],\"name\":\"transferredAddress\",\"outputs\":[{\"internalType\":\"address\",\"name\":\"\",\"type\":\"address\"}],\"stateMutability\":\"view\",\"type\":\"function\",\"constant\":true},{\"inputs\":[{\"internalType\":\"address\",\"name\":\"\",\"type\":\"address\"}],\"name\":\"transferredTotal\",\"outputs\":[{\"internalType\":\"uint256\",\"name\":\"\",\"type\":\"uint256\"}],\"stateMutability\":\"view\",\"type\":\"function\",\"constant\":true},{\"inputs\":[],\"name\":\"initialize\",\"outputs\":[],\"stateMutability\":\"nonpayable\",\"type\":\"function\"},{\"inputs\":[{\"internalType\":\"address\",\"name\":\"account\",\"type\":\"address\"}],\"name\":\"getTraffic\",\"outputs\":[{\"internalType\":\"uint256\",\"name\":\"\",\"type\":\"uint256\"}],\"stateMutability\":\"view\",\"type\":\"function\",\"constant\":true},{\"inputs\":[{\"internalType\":\"address\",\"name\":\"beneficiary\",\"type\":\"address\"},{\"internalType\":\"address\",\"name\":\"recipient\",\"type\":\"address\"},{\"internalType\":\"uint256\",\"name\":\"cumulativePayout\",\"type\":\"uint256\"},{\"internalType\":\"bytes\",\"name\":\"beneficiarySig\",\"type\":\"bytes\"}],\"name\":\"cashChequeBeneficiary\",\"outputs\":[],\"stateMutability\":\"nonpayable\",\"type\":\"function\"},{\"inputs\":[{\"internalType\":\"address\",\"name\":\"addr\",\"type\":\"address\"}],\"name\":\"getRetrievedAddress\",\"outputs\":[{\"internalType\":\"address[]\",\"name\":\"\",\"type\":\"address[]\"}],\"stateMutability\":\"view\",\"type\":\"function\",\"constant\":true},{\"inputs\":[{\"internalType\":\"address\",\"name\":\"addr\",\"type\":\"address\"}],\"name\":\"getTransferredAddress\",\"outputs\":[{\"internalType\":\"address[]\",\"name\":\"\",\"type\":\"address[]\"}],\"stateMutability\":\"view\",\"type\":\"function\",\"constant\":true}]",
}

// TrafficABI is the input ABI used to generate the binding from.
// Deprecated: Use TrafficMetaData.ABI instead.
var TrafficABI = TrafficMetaData.ABI

// Traffic is an auto generated Go binding around an Ethereum contract.
type Traffic struct {
	TrafficCaller     // Read-only binding to the contract
	TrafficTransactor // Write-only binding to the contract
	TrafficFilterer   // Log filterer for contract events
}

// TrafficCaller is an auto generated read-only Go binding around an Ethereum contract.
type TrafficCaller struct {
	contract *bind.BoundContract // Generic contract wrapper for the low level calls
}

// TrafficTransactor is an auto generated write-only Go binding around an Ethereum contract.
type TrafficTransactor struct {
	contract *bind.BoundContract // Generic contract wrapper for the low level calls
}

// TrafficFilterer is an auto generated log filtering Go binding around an Ethereum contract events.
type TrafficFilterer struct {
	contract *bind.BoundContract // Generic contract wrapper for the low level calls
}

// TrafficSession is an auto generated Go binding around an Ethereum contract,
// with pre-set call and transact options.
type TrafficSession struct {
	Contract     *Traffic          // Generic contract binding to set the session for
	CallOpts     bind.CallOpts     // Call options to use throughout this session
	TransactOpts bind.TransactOpts // Transaction auth options to use throughout this session
}

// TrafficCallerSession is an auto generated read-only Go binding around an Ethereum contract,
// with pre-set call options.
type TrafficCallerSession struct {
	Contract *TrafficCaller // Generic contract caller binding to set the session for
	CallOpts bind.CallOpts  // Call options to use throughout this session
}

// TrafficTransactorSession is an auto generated write-only Go binding around an Ethereum contract,
// with pre-set transact options.
type TrafficTransactorSession struct {
	Contract     *TrafficTransactor // Generic contract transactor binding to set the session for
	TransactOpts bind.TransactOpts  // Transaction auth options to use throughout this session
}

// TrafficRaw is an auto generated low-level Go binding around an Ethereum contract.
type TrafficRaw struct {
	Contract *Traffic // Generic contract binding to access the raw methods on
}

// TrafficCallerRaw is an auto generated low-level read-only Go binding around an Ethereum contract.
type TrafficCallerRaw struct {
	Contract *TrafficCaller // Generic read-only contract binding to access the raw methods on
}

// TrafficTransactorRaw is an auto generated low-level write-only Go binding around an Ethereum contract.
type TrafficTransactorRaw struct {
	Contract *TrafficTransactor // Generic write-only contract binding to access the raw methods on
}

// NewTraffic creates a new instance of Traffic, bound to a specific deployed contract.
func NewTraffic(address common.Address, backend bind.ContractBackend) (*Traffic, error) {
	contract, err := bindTraffic(address, backend, backend, backend)
	if err != nil {
		return nil, err
	}
	return &Traffic{TrafficCaller: TrafficCaller{contract: contract}, TrafficTransactor: TrafficTransactor{contract: contract}, TrafficFilterer: TrafficFilterer{contract: contract}}, nil
}

// NewTrafficCaller creates a new read-only instance of Traffic, bound to a specific deployed contract.
func NewTrafficCaller(address common.Address, caller bind.ContractCaller) (*TrafficCaller, error) {
	contract, err := bindTraffic(address, caller, nil, nil)
	if err != nil {
		return nil, err
	}
	return &TrafficCaller{contract: contract}, nil
}

// NewTrafficTransactor creates a new write-only instance of Traffic, bound to a specific deployed contract.
func NewTrafficTransactor(address common.Address, transactor bind.ContractTransactor) (*TrafficTransactor, error) {
	contract, err := bindTraffic(address, nil, transactor, nil)
	if err != nil {
		return nil, err
	}
	return &TrafficTransactor{contract: contract}, nil
}

// NewTrafficFilterer creates a new log filterer instance of Traffic, bound to a specific deployed contract.
func NewTrafficFilterer(address common.Address, filterer bind.ContractFilterer) (*TrafficFilterer, error) {
	contract, err := bindTraffic(address, nil, nil, filterer)
	if err != nil {
		return nil, err
	}
	return &TrafficFilterer{contract: contract}, nil
}

// bindTraffic binds a generic wrapper to an already deployed contract.
func bindTraffic(address common.Address, caller bind.ContractCaller, transactor bind.ContractTransactor, filterer bind.ContractFilterer) (*bind.BoundContract, error) {
	parsed, err := abi.JSON(strings.NewReader(TrafficABI))
	if err != nil {
		return nil, err
	}
	return bind.NewBoundContract(address, parsed, caller, transactor, filterer), nil
}

// Call invokes the (constant) contract method with params as input values and
// sets the output to result. The result type might be a single field for simple
// returns, a slice of interfaces for anonymous returns and a struct for named
// returns.
func (_Traffic *TrafficRaw) Call(opts *bind.CallOpts, result *[]interface{}, method string, params ...interface{}) error {
	return _Traffic.Contract.TrafficCaller.contract.Call(opts, result, method, params...)
}

// Transfer initiates a plain transaction to move funds to the contract, calling
// its default method if one is available.
func (_Traffic *TrafficRaw) Transfer(opts *bind.TransactOpts) (*types.Transaction, error) {
	return _Traffic.Contract.TrafficTransactor.contract.Transfer(opts)
}

// Transact invokes the (paid) contract method with params as input values.
func (_Traffic *TrafficRaw) Transact(opts *bind.TransactOpts, method string, params ...interface{}) (*types.Transaction, error) {
	return _Traffic.Contract.TrafficTransactor.contract.Transact(opts, method, params...)
}

// Call invokes the (constant) contract method with params as input values and
// sets the output to result. The result type might be a single field for simple
// returns, a slice of interfaces for anonymous returns and a struct for named
// returns.
func (_Traffic *TrafficCallerRaw) Call(opts *bind.CallOpts, result *[]interface{}, method string, params ...interface{}) error {
	return _Traffic.Contract.contract.Call(opts, result, method, params...)
}

// Transfer initiates a plain transaction to move funds to the contract, calling
// its default method if one is available.
func (_Traffic *TrafficTransactorRaw) Transfer(opts *bind.TransactOpts) (*types.Transaction, error) {
	return _Traffic.Contract.contract.Transfer(opts)
}

// Transact invokes the (paid) contract method with params as input values.
func (_Traffic *TrafficTransactorRaw) Transact(opts *bind.TransactOpts, method string, params ...interface{}) (*types.Transaction, error) {
	return _Traffic.Contract.contract.Transact(opts, method, params...)
}

// CHEQUETYPEHASH is a free data retrieval call binding the contract method 0x15c3343f.
//
// Solidity: function CHEQUE_TYPEHASH() view returns(bytes32)
func (_Traffic *TrafficCaller) CHEQUETYPEHASH(opts *bind.CallOpts) ([32]byte, error) {
	var out []interface{}
	err := _Traffic.contract.Call(opts, &out, "CHEQUE_TYPEHASH")

	if err != nil {
		return *new([32]byte), err
	}

	out0 := *abi.ConvertType(out[0], new([32]byte)).(*[32]byte)

	return out0, err

}

// CHEQUETYPEHASH is a free data retrieval call binding the contract method 0x15c3343f.
//
// Solidity: function CHEQUE_TYPEHASH() view returns(bytes32)
func (_Traffic *TrafficSession) CHEQUETYPEHASH() ([32]byte, error) {
	return _Traffic.Contract.CHEQUETYPEHASH(&_Traffic.CallOpts)
}

// CHEQUETYPEHASH is a free data retrieval call binding the contract method 0x15c3343f.
//
// Solidity: function CHEQUE_TYPEHASH() view returns(bytes32)
func (_Traffic *TrafficCallerSession) CHEQUETYPEHASH() ([32]byte, error) {
	return _Traffic.Contract.CHEQUETYPEHASH(&_Traffic.CallOpts)
}

// EIP712DOMAINTYPEHASH is a free data retrieval call binding the contract method 0xc49f91d3.
//
// Solidity: function EIP712DOMAIN_TYPEHASH() view returns(bytes32)
func (_Traffic *TrafficCaller) EIP712DOMAINTYPEHASH(opts *bind.CallOpts) ([32]byte, error) {
	var out []interface{}
	err := _Traffic.contract.Call(opts, &out, "EIP712DOMAIN_TYPEHASH")

	if err != nil {
		return *new([32]byte), err
	}

	out0 := *abi.ConvertType(out[0], new([32]byte)).(*[32]byte)

	return out0, err

}

// EIP712DOMAINTYPEHASH is a free data retrieval call binding the contract method 0xc49f91d3.
//
// Solidity: function EIP712DOMAIN_TYPEHASH() view returns(bytes32)
func (_Traffic *TrafficSession) EIP712DOMAINTYPEHASH() ([32]byte, error) {
	return _Traffic.Contract.EIP712DOMAINTYPEHASH(&_Traffic.CallOpts)
}

// EIP712DOMAINTYPEHASH is a free data retrieval call binding the contract method 0xc49f91d3.
//
// Solidity: function EIP712DOMAIN_TYPEHASH() view returns(bytes32)
func (_Traffic *TrafficCallerSession) EIP712DOMAINTYPEHASH() ([32]byte, error) {
	return _Traffic.Contract.EIP712DOMAINTYPEHASH(&_Traffic.CallOpts)
}

// Allowance is a free data retrieval call binding the contract method 0xdd62ed3e.
//
// Solidity: function allowance(address owner, address spender) view returns(uint256)
func (_Traffic *TrafficCaller) Allowance(opts *bind.CallOpts, owner common.Address, spender common.Address) (*big.Int, error) {
	var out []interface{}
	err := _Traffic.contract.Call(opts, &out, "allowance", owner, spender)

	if err != nil {
		return *new(*big.Int), err
	}

	out0 := *abi.ConvertType(out[0], new(*big.Int)).(**big.Int)

	return out0, err

}

// Allowance is a free data retrieval call binding the contract method 0xdd62ed3e.
//
// Solidity: function allowance(address owner, address spender) view returns(uint256)
func (_Traffic *TrafficSession) Allowance(owner common.Address, spender common.Address) (*big.Int, error) {
	return _Traffic.Contract.Allowance(&_Traffic.CallOpts, owner, spender)
}

// Allowance is a free data retrieval call binding the contract method 0xdd62ed3e.
//
// Solidity: function allowance(address owner, address spender) view returns(uint256)
func (_Traffic *TrafficCallerSession) Allowance(owner common.Address, spender common.Address) (*big.Int, error) {
	return _Traffic.Contract.Allowance(&_Traffic.CallOpts, owner, spender)
}

// BalanceOf is a free data retrieval call binding the contract method 0x70a08231.
//
// Solidity: function balanceOf(address account) view returns(uint256)
func (_Traffic *TrafficCaller) BalanceOf(opts *bind.CallOpts, account common.Address) (*big.Int, error) {
	var out []interface{}
	err := _Traffic.contract.Call(opts, &out, "balanceOf", account)

	if err != nil {
		return *new(*big.Int), err
	}

	out0 := *abi.ConvertType(out[0], new(*big.Int)).(**big.Int)

	return out0, err

}

// BalanceOf is a free data retrieval call binding the contract method 0x70a08231.
//
// Solidity: function balanceOf(address account) view returns(uint256)
func (_Traffic *TrafficSession) BalanceOf(account common.Address) (*big.Int, error) {
	return _Traffic.Contract.BalanceOf(&_Traffic.CallOpts, account)
}

// BalanceOf is a free data retrieval call binding the contract method 0x70a08231.
//
// Solidity: function balanceOf(address account) view returns(uint256)
func (_Traffic *TrafficCallerSession) BalanceOf(account common.Address) (*big.Int, error) {
	return _Traffic.Contract.BalanceOf(&_Traffic.CallOpts, account)
}

// Decimals is a free data retrieval call binding the contract method 0x313ce567.
//
// Solidity: function decimals() view returns(uint8)
func (_Traffic *TrafficCaller) Decimals(opts *bind.CallOpts) (uint8, error) {
	var out []interface{}
	err := _Traffic.contract.Call(opts, &out, "decimals")

	if err != nil {
		return *new(uint8), err
	}

	out0 := *abi.ConvertType(out[0], new(uint8)).(*uint8)

	return out0, err

}

// Decimals is a free data retrieval call binding the contract method 0x313ce567.
//
// Solidity: function decimals() view returns(uint8)
func (_Traffic *TrafficSession) Decimals() (uint8, error) {
	return _Traffic.Contract.Decimals(&_Traffic.CallOpts)
}

// Decimals is a free data retrieval call binding the contract method 0x313ce567.
//
// Solidity: function decimals() view returns(uint8)
func (_Traffic *TrafficCallerSession) Decimals() (uint8, error) {
	return _Traffic.Contract.Decimals(&_Traffic.CallOpts)
}

// GetRetrievedAddress is a free data retrieval call binding the contract method 0x2f5c1cab.
//
// Solidity: function getRetrievedAddress(address addr) view returns(address[])
func (_Traffic *TrafficCaller) GetRetrievedAddress(opts *bind.CallOpts, addr common.Address) ([]common.Address, error) {
	var out []interface{}
	err := _Traffic.contract.Call(opts, &out, "getRetrievedAddress", addr)

	if err != nil {
		return *new([]common.Address), err
	}

	out0 := *abi.ConvertType(out[0], new([]common.Address)).(*[]common.Address)

	return out0, err

}

// GetRetrievedAddress is a free data retrieval call binding the contract method 0x2f5c1cab.
//
// Solidity: function getRetrievedAddress(address addr) view returns(address[])
func (_Traffic *TrafficSession) GetRetrievedAddress(addr common.Address) ([]common.Address, error) {
	return _Traffic.Contract.GetRetrievedAddress(&_Traffic.CallOpts, addr)
}

// GetRetrievedAddress is a free data retrieval call binding the contract method 0x2f5c1cab.
//
// Solidity: function getRetrievedAddress(address addr) view returns(address[])
func (_Traffic *TrafficCallerSession) GetRetrievedAddress(addr common.Address) ([]common.Address, error) {
	return _Traffic.Contract.GetRetrievedAddress(&_Traffic.CallOpts, addr)
}

// GetTraffic is a free data retrieval call binding the contract method 0xd2615bab.
//
// Solidity: function getTraffic(address account) view returns(uint256)
func (_Traffic *TrafficCaller) GetTraffic(opts *bind.CallOpts, account common.Address) (*big.Int, error) {
	var out []interface{}
	err := _Traffic.contract.Call(opts, &out, "getTraffic", account)

	if err != nil {
		return *new(*big.Int), err
	}

	out0 := *abi.ConvertType(out[0], new(*big.Int)).(**big.Int)

	return out0, err

}

// GetTraffic is a free data retrieval call binding the contract method 0xd2615bab.
//
// Solidity: function getTraffic(address account) view returns(uint256)
func (_Traffic *TrafficSession) GetTraffic(account common.Address) (*big.Int, error) {
	return _Traffic.Contract.GetTraffic(&_Traffic.CallOpts, account)
}

// GetTraffic is a free data retrieval call binding the contract method 0xd2615bab.
//
// Solidity: function getTraffic(address account) view returns(uint256)
func (_Traffic *TrafficCallerSession) GetTraffic(account common.Address) (*big.Int, error) {
	return _Traffic.Contract.GetTraffic(&_Traffic.CallOpts, account)
}

// GetTransferredAddress is a free data retrieval call binding the contract method 0xc10b879a.
//
// Solidity: function getTransferredAddress(address addr) view returns(address[])
func (_Traffic *TrafficCaller) GetTransferredAddress(opts *bind.CallOpts, addr common.Address) ([]common.Address, error) {
	var out []interface{}
	err := _Traffic.contract.Call(opts, &out, "getTransferredAddress", addr)

	if err != nil {
		return *new([]common.Address), err
	}

	out0 := *abi.ConvertType(out[0], new([]common.Address)).(*[]common.Address)

	return out0, err

}

// GetTransferredAddress is a free data retrieval call binding the contract method 0xc10b879a.
//
// Solidity: function getTransferredAddress(address addr) view returns(address[])
func (_Traffic *TrafficSession) GetTransferredAddress(addr common.Address) ([]common.Address, error) {
	return _Traffic.Contract.GetTransferredAddress(&_Traffic.CallOpts, addr)
}

// GetTransferredAddress is a free data retrieval call binding the contract method 0xc10b879a.
//
// Solidity: function getTransferredAddress(address addr) view returns(address[])
func (_Traffic *TrafficCallerSession) GetTransferredAddress(addr common.Address) ([]common.Address, error) {
	return _Traffic.Contract.GetTransferredAddress(&_Traffic.CallOpts, addr)
}

// Name is a free data retrieval call binding the contract method 0x06fdde03.
//
// Solidity: function name() view returns(string)
func (_Traffic *TrafficCaller) Name(opts *bind.CallOpts) (string, error) {
	var out []interface{}
	err := _Traffic.contract.Call(opts, &out, "name")

	if err != nil {
		return *new(string), err
	}

	out0 := *abi.ConvertType(out[0], new(string)).(*string)

	return out0, err

}

// Name is a free data retrieval call binding the contract method 0x06fdde03.
//
// Solidity: function name() view returns(string)
func (_Traffic *TrafficSession) Name() (string, error) {
	return _Traffic.Contract.Name(&_Traffic.CallOpts)
}

// Name is a free data retrieval call binding the contract method 0x06fdde03.
//
// Solidity: function name() view returns(string)
func (_Traffic *TrafficCallerSession) Name() (string, error) {
	return _Traffic.Contract.Name(&_Traffic.CallOpts)
}

// Owner is a free data retrieval call binding the contract method 0x8da5cb5b.
//
// Solidity: function owner() view returns(address)
func (_Traffic *TrafficCaller) Owner(opts *bind.CallOpts) (common.Address, error) {
	var out []interface{}
	err := _Traffic.contract.Call(opts, &out, "owner")

	if err != nil {
		return *new(common.Address), err
	}

	out0 := *abi.ConvertType(out[0], new(common.Address)).(*common.Address)

	return out0, err

}

// Owner is a free data retrieval call binding the contract method 0x8da5cb5b.
//
// Solidity: function owner() view returns(address)
func (_Traffic *TrafficSession) Owner() (common.Address, error) {
	return _Traffic.Contract.Owner(&_Traffic.CallOpts)
}

// Owner is a free data retrieval call binding the contract method 0x8da5cb5b.
//
// Solidity: function owner() view returns(address)
func (_Traffic *TrafficCallerSession) Owner() (common.Address, error) {
	return _Traffic.Contract.Owner(&_Traffic.CallOpts)
}

// RetrievedAddress is a free data retrieval call binding the contract method 0xe7271ccc.
//
// Solidity: function retrievedAddress(address , uint256 ) view returns(address)
func (_Traffic *TrafficCaller) RetrievedAddress(opts *bind.CallOpts, arg0 common.Address, arg1 *big.Int) (common.Address, error) {
	var out []interface{}
	err := _Traffic.contract.Call(opts, &out, "retrievedAddress", arg0, arg1)

	if err != nil {
		return *new(common.Address), err
	}

	out0 := *abi.ConvertType(out[0], new(common.Address)).(*common.Address)

	return out0, err

}

// RetrievedAddress is a free data retrieval call binding the contract method 0xe7271ccc.
//
// Solidity: function retrievedAddress(address , uint256 ) view returns(address)
func (_Traffic *TrafficSession) RetrievedAddress(arg0 common.Address, arg1 *big.Int) (common.Address, error) {
	return _Traffic.Contract.RetrievedAddress(&_Traffic.CallOpts, arg0, arg1)
}

// RetrievedAddress is a free data retrieval call binding the contract method 0xe7271ccc.
//
// Solidity: function retrievedAddress(address , uint256 ) view returns(address)
func (_Traffic *TrafficCallerSession) RetrievedAddress(arg0 common.Address, arg1 *big.Int) (common.Address, error) {
	return _Traffic.Contract.RetrievedAddress(&_Traffic.CallOpts, arg0, arg1)
}

// RetrievedTotal is a free data retrieval call binding the contract method 0xf5822d0f.
//
// Solidity: function retrievedTotal(address ) view returns(uint256)
func (_Traffic *TrafficCaller) RetrievedTotal(opts *bind.CallOpts, arg0 common.Address) (*big.Int, error) {
	var out []interface{}
	err := _Traffic.contract.Call(opts, &out, "retrievedTotal", arg0)

	if err != nil {
		return *new(*big.Int), err
	}

	out0 := *abi.ConvertType(out[0], new(*big.Int)).(**big.Int)

	return out0, err

}

// RetrievedTotal is a free data retrieval call binding the contract method 0xf5822d0f.
//
// Solidity: function retrievedTotal(address ) view returns(uint256)
func (_Traffic *TrafficSession) RetrievedTotal(arg0 common.Address) (*big.Int, error) {
	return _Traffic.Contract.RetrievedTotal(&_Traffic.CallOpts, arg0)
}

// RetrievedTotal is a free data retrieval call binding the contract method 0xf5822d0f.
//
// Solidity: function retrievedTotal(address ) view returns(uint256)
func (_Traffic *TrafficCallerSession) RetrievedTotal(arg0 common.Address) (*big.Int, error) {
	return _Traffic.Contract.RetrievedTotal(&_Traffic.CallOpts, arg0)
}

// Symbol is a free data retrieval call binding the contract method 0x95d89b41.
//
// Solidity: function symbol() view returns(string)
func (_Traffic *TrafficCaller) Symbol(opts *bind.CallOpts) (string, error) {
	var out []interface{}
	err := _Traffic.contract.Call(opts, &out, "symbol")

	if err != nil {
		return *new(string), err
	}

	out0 := *abi.ConvertType(out[0], new(string)).(*string)

	return out0, err

}

// Symbol is a free data retrieval call binding the contract method 0x95d89b41.
//
// Solidity: function symbol() view returns(string)
func (_Traffic *TrafficSession) Symbol() (string, error) {
	return _Traffic.Contract.Symbol(&_Traffic.CallOpts)
}

// Symbol is a free data retrieval call binding the contract method 0x95d89b41.
//
// Solidity: function symbol() view returns(string)
func (_Traffic *TrafficCallerSession) Symbol() (string, error) {
	return _Traffic.Contract.Symbol(&_Traffic.CallOpts)
}

// TotalSupply is a free data retrieval call binding the contract method 0x18160ddd.
//
// Solidity: function totalSupply() view returns(uint256)
func (_Traffic *TrafficCaller) TotalSupply(opts *bind.CallOpts) (*big.Int, error) {
	var out []interface{}
	err := _Traffic.contract.Call(opts, &out, "totalSupply")

	if err != nil {
		return *new(*big.Int), err
	}

	out0 := *abi.ConvertType(out[0], new(*big.Int)).(**big.Int)

	return out0, err

}

// TotalSupply is a free data retrieval call binding the contract method 0x18160ddd.
//
// Solidity: function totalSupply() view returns(uint256)
func (_Traffic *TrafficSession) TotalSupply() (*big.Int, error) {
	return _Traffic.Contract.TotalSupply(&_Traffic.CallOpts)
}

// TotalSupply is a free data retrieval call binding the contract method 0x18160ddd.
//
// Solidity: function totalSupply() view returns(uint256)
func (_Traffic *TrafficCallerSession) TotalSupply() (*big.Int, error) {
	return _Traffic.Contract.TotalSupply(&_Traffic.CallOpts)
}

// TransTraffic is a free data retrieval call binding the contract method 0xa12a75b9.
//
// Solidity: function transTraffic(address , address ) view returns(uint256)
func (_Traffic *TrafficCaller) TransTraffic(opts *bind.CallOpts, arg0 common.Address, arg1 common.Address) (*big.Int, error) {
	var out []interface{}
	err := _Traffic.contract.Call(opts, &out, "transTraffic", arg0, arg1)

	if err != nil {
		return *new(*big.Int), err
	}

	out0 := *abi.ConvertType(out[0], new(*big.Int)).(**big.Int)

	return out0, err

}

// TransTraffic is a free data retrieval call binding the contract method 0xa12a75b9.
//
// Solidity: function transTraffic(address , address ) view returns(uint256)
func (_Traffic *TrafficSession) TransTraffic(arg0 common.Address, arg1 common.Address) (*big.Int, error) {
	return _Traffic.Contract.TransTraffic(&_Traffic.CallOpts, arg0, arg1)
}

// TransTraffic is a free data retrieval call binding the contract method 0xa12a75b9.
//
// Solidity: function transTraffic(address , address ) view returns(uint256)
func (_Traffic *TrafficCallerSession) TransTraffic(arg0 common.Address, arg1 common.Address) (*big.Int, error) {
	return _Traffic.Contract.TransTraffic(&_Traffic.CallOpts, arg0, arg1)
}

// TransferredAddress is a free data retrieval call binding the contract method 0xdafedb4a.
//
// Solidity: function transferredAddress(address , uint256 ) view returns(address)
func (_Traffic *TrafficCaller) TransferredAddress(opts *bind.CallOpts, arg0 common.Address, arg1 *big.Int) (common.Address, error) {
	var out []interface{}
	err := _Traffic.contract.Call(opts, &out, "transferredAddress", arg0, arg1)

	if err != nil {
		return *new(common.Address), err
	}

	out0 := *abi.ConvertType(out[0], new(common.Address)).(*common.Address)

	return out0, err

}

// TransferredAddress is a free data retrieval call binding the contract method 0xdafedb4a.
//
// Solidity: function transferredAddress(address , uint256 ) view returns(address)
func (_Traffic *TrafficSession) TransferredAddress(arg0 common.Address, arg1 *big.Int) (common.Address, error) {
	return _Traffic.Contract.TransferredAddress(&_Traffic.CallOpts, arg0, arg1)
}

// TransferredAddress is a free data retrieval call binding the contract method 0xdafedb4a.
//
// Solidity: function transferredAddress(address , uint256 ) view returns(address)
func (_Traffic *TrafficCallerSession) TransferredAddress(arg0 common.Address, arg1 *big.Int) (common.Address, error) {
	return _Traffic.Contract.TransferredAddress(&_Traffic.CallOpts, arg0, arg1)
}

// TransferredTotal is a free data retrieval call binding the contract method 0xd4eac37f.
//
// Solidity: function transferredTotal(address ) view returns(uint256)
func (_Traffic *TrafficCaller) TransferredTotal(opts *bind.CallOpts, arg0 common.Address) (*big.Int, error) {
	var out []interface{}
	err := _Traffic.contract.Call(opts, &out, "transferredTotal", arg0)

	if err != nil {
		return *new(*big.Int), err
	}

	out0 := *abi.ConvertType(out[0], new(*big.Int)).(**big.Int)

	return out0, err

}

// TransferredTotal is a free data retrieval call binding the contract method 0xd4eac37f.
//
// Solidity: function transferredTotal(address ) view returns(uint256)
func (_Traffic *TrafficSession) TransferredTotal(arg0 common.Address) (*big.Int, error) {
	return _Traffic.Contract.TransferredTotal(&_Traffic.CallOpts, arg0)
}

// TransferredTotal is a free data retrieval call binding the contract method 0xd4eac37f.
//
// Solidity: function transferredTotal(address ) view returns(uint256)
func (_Traffic *TrafficCallerSession) TransferredTotal(arg0 common.Address) (*big.Int, error) {
	return _Traffic.Contract.TransferredTotal(&_Traffic.CallOpts, arg0)
}

// Approve is a paid mutator transaction binding the contract method 0x095ea7b3.
//
// Solidity: function approve(address spender, uint256 amount) returns(bool)
func (_Traffic *TrafficTransactor) Approve(opts *bind.TransactOpts, spender common.Address, amount *big.Int) (*types.Transaction, error) {
	return _Traffic.contract.Transact(opts, "approve", spender, amount)
}

// Approve is a paid mutator transaction binding the contract method 0x095ea7b3.
//
// Solidity: function approve(address spender, uint256 amount) returns(bool)
func (_Traffic *TrafficSession) Approve(spender common.Address, amount *big.Int) (*types.Transaction, error) {
	return _Traffic.Contract.Approve(&_Traffic.TransactOpts, spender, amount)
}

// Approve is a paid mutator transaction binding the contract method 0x095ea7b3.
//
// Solidity: function approve(address spender, uint256 amount) returns(bool)
func (_Traffic *TrafficTransactorSession) Approve(spender common.Address, amount *big.Int) (*types.Transaction, error) {
	return _Traffic.Contract.Approve(&_Traffic.TransactOpts, spender, amount)
}

// CashChequeBeneficiary is a paid mutator transaction binding the contract method 0xc595fa74.
//
// Solidity: function cashChequeBeneficiary(address beneficiary, address recipient, uint256 cumulativePayout, bytes beneficiarySig) returns()
func (_Traffic *TrafficTransactor) CashChequeBeneficiary(opts *bind.TransactOpts, beneficiary common.Address, recipient common.Address, cumulativePayout *big.Int, beneficiarySig []byte) (*types.Transaction, error) {
	return _Traffic.contract.Transact(opts, "cashChequeBeneficiary", beneficiary, recipient, cumulativePayout, beneficiarySig)
}

// CashChequeBeneficiary is a paid mutator transaction binding the contract method 0xc595fa74.
//
// Solidity: function cashChequeBeneficiary(address beneficiary, address recipient, uint256 cumulativePayout, bytes beneficiarySig) returns()
func (_Traffic *TrafficSession) CashChequeBeneficiary(beneficiary common.Address, recipient common.Address, cumulativePayout *big.Int, beneficiarySig []byte) (*types.Transaction, error) {
	return _Traffic.Contract.CashChequeBeneficiary(&_Traffic.TransactOpts, beneficiary, recipient, cumulativePayout, beneficiarySig)
}

// CashChequeBeneficiary is a paid mutator transaction binding the contract method 0xc595fa74.
//
// Solidity: function cashChequeBeneficiary(address beneficiary, address recipient, uint256 cumulativePayout, bytes beneficiarySig) returns()
func (_Traffic *TrafficTransactorSession) CashChequeBeneficiary(beneficiary common.Address, recipient common.Address, cumulativePayout *big.Int, beneficiarySig []byte) (*types.Transaction, error) {
	return _Traffic.Contract.CashChequeBeneficiary(&_Traffic.TransactOpts, beneficiary, recipient, cumulativePayout, beneficiarySig)
}

// DecreaseAllowance is a paid mutator transaction binding the contract method 0xa457c2d7.
//
// Solidity: function decreaseAllowance(address spender, uint256 subtractedValue) returns(bool)
func (_Traffic *TrafficTransactor) DecreaseAllowance(opts *bind.TransactOpts, spender common.Address, subtractedValue *big.Int) (*types.Transaction, error) {
	return _Traffic.contract.Transact(opts, "decreaseAllowance", spender, subtractedValue)
}

// DecreaseAllowance is a paid mutator transaction binding the contract method 0xa457c2d7.
//
// Solidity: function decreaseAllowance(address spender, uint256 subtractedValue) returns(bool)
func (_Traffic *TrafficSession) DecreaseAllowance(spender common.Address, subtractedValue *big.Int) (*types.Transaction, error) {
	return _Traffic.Contract.DecreaseAllowance(&_Traffic.TransactOpts, spender, subtractedValue)
}

// DecreaseAllowance is a paid mutator transaction binding the contract method 0xa457c2d7.
//
// Solidity: function decreaseAllowance(address spender, uint256 subtractedValue) returns(bool)
func (_Traffic *TrafficTransactorSession) DecreaseAllowance(spender common.Address, subtractedValue *big.Int) (*types.Transaction, error) {
	return _Traffic.Contract.DecreaseAllowance(&_Traffic.TransactOpts, spender, subtractedValue)
}

// IncreaseAllowance is a paid mutator transaction binding the contract method 0x39509351.
//
// Solidity: function increaseAllowance(address spender, uint256 addedValue) returns(bool)
func (_Traffic *TrafficTransactor) IncreaseAllowance(opts *bind.TransactOpts, spender common.Address, addedValue *big.Int) (*types.Transaction, error) {
	return _Traffic.contract.Transact(opts, "increaseAllowance", spender, addedValue)
}

// IncreaseAllowance is a paid mutator transaction binding the contract method 0x39509351.
//
// Solidity: function increaseAllowance(address spender, uint256 addedValue) returns(bool)
func (_Traffic *TrafficSession) IncreaseAllowance(spender common.Address, addedValue *big.Int) (*types.Transaction, error) {
	return _Traffic.Contract.IncreaseAllowance(&_Traffic.TransactOpts, spender, addedValue)
}

// IncreaseAllowance is a paid mutator transaction binding the contract method 0x39509351.
//
// Solidity: function increaseAllowance(address spender, uint256 addedValue) returns(bool)
func (_Traffic *TrafficTransactorSession) IncreaseAllowance(spender common.Address, addedValue *big.Int) (*types.Transaction, error) {
	return _Traffic.Contract.IncreaseAllowance(&_Traffic.TransactOpts, spender, addedValue)
}

// Initialize is a paid mutator transaction binding the contract method 0x8129fc1c.
//
// Solidity: function initialize() returns()
func (_Traffic *TrafficTransactor) Initialize(opts *bind.TransactOpts) (*types.Transaction, error) {
	return _Traffic.contract.Transact(opts, "initialize")
}

// Initialize is a paid mutator transaction binding the contract method 0x8129fc1c.
//
// Solidity: function initialize() returns()
func (_Traffic *TrafficSession) Initialize() (*types.Transaction, error) {
	return _Traffic.Contract.Initialize(&_Traffic.TransactOpts)
}

// Initialize is a paid mutator transaction binding the contract method 0x8129fc1c.
//
// Solidity: function initialize() returns()
func (_Traffic *TrafficTransactorSession) Initialize() (*types.Transaction, error) {
	return _Traffic.Contract.Initialize(&_Traffic.TransactOpts)
}

// Transfer is a paid mutator transaction binding the contract method 0xa9059cbb.
//
// Solidity: function transfer(address recipient, uint256 amount) returns(bool)
func (_Traffic *TrafficTransactor) Transfer(opts *bind.TransactOpts, recipient common.Address, amount *big.Int) (*types.Transaction, error) {
	return _Traffic.contract.Transact(opts, "transfer", recipient, amount)
}

// Transfer is a paid mutator transaction binding the contract method 0xa9059cbb.
//
// Solidity: function transfer(address recipient, uint256 amount) returns(bool)
func (_Traffic *TrafficSession) Transfer(recipient common.Address, amount *big.Int) (*types.Transaction, error) {
	return _Traffic.Contract.Transfer(&_Traffic.TransactOpts, recipient, amount)
}

// Transfer is a paid mutator transaction binding the contract method 0xa9059cbb.
//
// Solidity: function transfer(address recipient, uint256 amount) returns(bool)
func (_Traffic *TrafficTransactorSession) Transfer(recipient common.Address, amount *big.Int) (*types.Transaction, error) {
	return _Traffic.Contract.Transfer(&_Traffic.TransactOpts, recipient, amount)
}

// TransferFrom is a paid mutator transaction binding the contract method 0x23b872dd.
//
// Solidity: function transferFrom(address sender, address recipient, uint256 amount) returns(bool)
func (_Traffic *TrafficTransactor) TransferFrom(opts *bind.TransactOpts, sender common.Address, recipient common.Address, amount *big.Int) (*types.Transaction, error) {
	return _Traffic.contract.Transact(opts, "transferFrom", sender, recipient, amount)
}

// TransferFrom is a paid mutator transaction binding the contract method 0x23b872dd.
//
// Solidity: function transferFrom(address sender, address recipient, uint256 amount) returns(bool)
func (_Traffic *TrafficSession) TransferFrom(sender common.Address, recipient common.Address, amount *big.Int) (*types.Transaction, error) {
	return _Traffic.Contract.TransferFrom(&_Traffic.TransactOpts, sender, recipient, amount)
}

// TransferFrom is a paid mutator transaction binding the contract method 0x23b872dd.
//
// Solidity: function transferFrom(address sender, address recipient, uint256 amount) returns(bool)
func (_Traffic *TrafficTransactorSession) TransferFrom(sender common.Address, recipient common.Address, amount *big.Int) (*types.Transaction, error) {
	return _Traffic.Contract.TransferFrom(&_Traffic.TransactOpts, sender, recipient, amount)
}

// TrafficApprovalIterator is returned from FilterApproval and is used to iterate over the raw logs and unpacked data for Approval events raised by the Traffic contract.
type TrafficApprovalIterator struct {
	Event *TrafficApproval // Event containing the contract specifics and raw log

	contract *bind.BoundContract // Generic contract to use for unpacking event data
	event    string              // Event name to use for unpacking event data

	logs chan types.Log        // Log channel receiving the found contract events
	sub  ethereum.Subscription // Subscription for errors, completion and termination
	done bool                  // Whether the subscription completed delivering logs
	fail error                 // Occurred error to stop iteration
}

// Next advances the iterator to the subsequent event, returning whether there
// are any more events found. In case of a retrieval or parsing error, false is
// returned and Error() can be queried for the exact failure.
func (it *TrafficApprovalIterator) Next() bool {
	// If the iterator failed, stop iterating
	if it.fail != nil {
		return false
	}
	// If the iterator completed, deliver directly whatever's available
	if it.done {
		select {
		case log := <-it.logs:
			it.Event = new(TrafficApproval)
			if err := it.contract.UnpackLog(it.Event, it.event, log); err != nil {
				it.fail = err
				return false
			}
			it.Event.Raw = log
			return true

		default:
			return false
		}
	}
	// Iterator still in progress, wait for either a data or an error event
	select {
	case log := <-it.logs:
		it.Event = new(TrafficApproval)
		if err := it.contract.UnpackLog(it.Event, it.event, log); err != nil {
			it.fail = err
			return false
		}
		it.Event.Raw = log
		return true

	case err := <-it.sub.Err():
		it.done = true
		it.fail = err
		return it.Next()
	}
}

// Error returns any retrieval or parsing error occurred during filtering.
func (it *TrafficApprovalIterator) Error() error {
	return it.fail
}

// Close terminates the iteration process, releasing any pending underlying
// resources.
func (it *TrafficApprovalIterator) Close() error {
	it.sub.Unsubscribe()
	return nil
}

// TrafficApproval represents a Approval event raised by the Traffic contract.
type TrafficApproval struct {
	Owner   common.Address
	Spender common.Address
	Value   *big.Int
	Raw     types.Log // Blockchain specific contextual infos
}

// FilterApproval is a free log retrieval operation binding the contract event 0x8c5be1e5ebec7d5bd14f71427d1e84f3dd0314c0f7b2291e5b200ac8c7c3b925.
//
// Solidity: event Approval(address indexed owner, address indexed spender, uint256 value)
func (_Traffic *TrafficFilterer) FilterApproval(opts *bind.FilterOpts, owner []common.Address, spender []common.Address) (*TrafficApprovalIterator, error) {

	var ownerRule []interface{}
	for _, ownerItem := range owner {
		ownerRule = append(ownerRule, ownerItem)
	}
	var spenderRule []interface{}
	for _, spenderItem := range spender {
		spenderRule = append(spenderRule, spenderItem)
	}

	logs, sub, err := _Traffic.contract.FilterLogs(opts, "Approval", ownerRule, spenderRule)
	if err != nil {
		return nil, err
	}
	return &TrafficApprovalIterator{contract: _Traffic.contract, event: "Approval", logs: logs, sub: sub}, nil
}

// WatchApproval is a free log subscription operation binding the contract event 0x8c5be1e5ebec7d5bd14f71427d1e84f3dd0314c0f7b2291e5b200ac8c7c3b925.
//
// Solidity: event Approval(address indexed owner, address indexed spender, uint256 value)
func (_Traffic *TrafficFilterer) WatchApproval(opts *bind.WatchOpts, sink chan<- *TrafficApproval, owner []common.Address, spender []common.Address) (event.Subscription, error) {

	var ownerRule []interface{}
	for _, ownerItem := range owner {
		ownerRule = append(ownerRule, ownerItem)
	}
	var spenderRule []interface{}
	for _, spenderItem := range spender {
		spenderRule = append(spenderRule, spenderItem)
	}

	logs, sub, err := _Traffic.contract.WatchLogs(opts, "Approval", ownerRule, spenderRule)
	if err != nil {
		return nil, err
	}
	return event.NewSubscription(func(quit <-chan struct{}) error {
		defer sub.Unsubscribe()
		for {
			select {
			case log := <-logs:
				// New log arrived, parse the event and forward to the user
				event := new(TrafficApproval)
				if err := _Traffic.contract.UnpackLog(event, "Approval", log); err != nil {
					return err
				}
				event.Raw = log

				select {
				case sink <- event:
				case err := <-sub.Err():
					return err
				case <-quit:
					return nil
				}
			case err := <-sub.Err():
				return err
			case <-quit:
				return nil
			}
		}
	}), nil
}

// ParseApproval is a log parse operation binding the contract event 0x8c5be1e5ebec7d5bd14f71427d1e84f3dd0314c0f7b2291e5b200ac8c7c3b925.
//
// Solidity: event Approval(address indexed owner, address indexed spender, uint256 value)
func (_Traffic *TrafficFilterer) ParseApproval(log types.Log) (*TrafficApproval, error) {
	event := new(TrafficApproval)
	if err := _Traffic.contract.UnpackLog(event, "Approval", log); err != nil {
		return nil, err
	}
	event.Raw = log
	return event, nil
}

// TrafficTransferIterator is returned from FilterTransfer and is used to iterate over the raw logs and unpacked data for Transfer events raised by the Traffic contract.
type TrafficTransferIterator struct {
	Event *TrafficTransfer // Event containing the contract specifics and raw log

	contract *bind.BoundContract // Generic contract to use for unpacking event data
	event    string              // Event name to use for unpacking event data

	logs chan types.Log        // Log channel receiving the found contract events
	sub  ethereum.Subscription // Subscription for errors, completion and termination
	done bool                  // Whether the subscription completed delivering logs
	fail error                 // Occurred error to stop iteration
}

// Next advances the iterator to the subsequent event, returning whether there
// are any more events found. In case of a retrieval or parsing error, false is
// returned and Error() can be queried for the exact failure.
func (it *TrafficTransferIterator) Next() bool {
	// If the iterator failed, stop iterating
	if it.fail != nil {
		return false
	}
	// If the iterator completed, deliver directly whatever's available
	if it.done {
		select {
		case log := <-it.logs:
			it.Event = new(TrafficTransfer)
			if err := it.contract.UnpackLog(it.Event, it.event, log); err != nil {
				it.fail = err
				return false
			}
			it.Event.Raw = log
			return true

		default:
			return false
		}
	}
	// Iterator still in progress, wait for either a data or an error event
	select {
	case log := <-it.logs:
		it.Event = new(TrafficTransfer)
		if err := it.contract.UnpackLog(it.Event, it.event, log); err != nil {
			it.fail = err
			return false
		}
		it.Event.Raw = log
		return true

	case err := <-it.sub.Err():
		it.done = true
		it.fail = err
		return it.Next()
	}
}

// Error returns any retrieval or parsing error occurred during filtering.
func (it *TrafficTransferIterator) Error() error {
	return it.fail
}

// Close terminates the iteration process, releasing any pending underlying
// resources.
func (it *TrafficTransferIterator) Close() error {
	it.sub.Unsubscribe()
	return nil
}

// TrafficTransfer represents a Transfer event raised by the Traffic contract.
type TrafficTransfer struct {
	From  common.Address
	To    common.Address
	Value *big.Int
	Raw   types.Log // Blockchain specific contextual infos
}

// FilterTransfer is a free log retrieval operation binding the contract event 0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef.
//
// Solidity: event Transfer(address indexed from, address indexed to, uint256 value)
func (_Traffic *TrafficFilterer) FilterTransfer(opts *bind.FilterOpts, from []common.Address, to []common.Address) (*TrafficTransferIterator, error) {

	var fromRule []interface{}
	for _, fromItem := range from {
		fromRule = append(fromRule, fromItem)
	}
	var toRule []interface{}
	for _, toItem := range to {
		toRule = append(toRule, toItem)
	}

	logs, sub, err := _Traffic.contract.FilterLogs(opts, "Transfer", fromRule, toRule)
	if err != nil {
		return nil, err
	}
	return &TrafficTransferIterator{contract: _Traffic.contract, event: "Transfer", logs: logs, sub: sub}, nil
}

// WatchTransfer is a free log subscription operation binding the contract event 0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef.
//
// Solidity: event Transfer(address indexed from, address indexed to, uint256 value)
func (_Traffic *TrafficFilterer) WatchTransfer(opts *bind.WatchOpts, sink chan<- *TrafficTransfer, from []common.Address, to []common.Address) (event.Subscription, error) {

	var fromRule []interface{}
	for _, fromItem := range from {
		fromRule = append(fromRule, fromItem)
	}
	var toRule []interface{}
	for _, toItem := range to {
		toRule = append(toRule, toItem)
	}

	logs, sub, err := _Traffic.contract.WatchLogs(opts, "Transfer", fromRule, toRule)
	if err != nil {
		return nil, err
	}
	return event.NewSubscription(func(quit <-chan struct{}) error {
		defer sub.Unsubscribe()
		for {
			select {
			case log := <-logs:
				// New log arrived, parse the event and forward to the user
				event := new(TrafficTransfer)
				if err := _Traffic.contract.UnpackLog(event, "Transfer", log); err != nil {
					return err
				}
				event.Raw = log

				select {
				case sink <- event:
				case err := <-sub.Err():
					return err
				case <-quit:
					return nil
				}
			case err := <-sub.Err():
				return err
			case <-quit:
				return nil
			}
		}
	}), nil
}

// ParseTransfer is a log parse operation binding the contract event 0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef.
//
// Solidity: event Transfer(address indexed from, address indexed to, uint256 value)
func (_Traffic *TrafficFilterer) ParseTransfer(log types.Log) (*TrafficTransfer, error) {
	event := new(TrafficTransfer)
	if err := _Traffic.contract.UnpackLog(event, "Transfer", log); err != nil {
		return nil, err
	}
	event.Raw = log
	return event, nil
}
