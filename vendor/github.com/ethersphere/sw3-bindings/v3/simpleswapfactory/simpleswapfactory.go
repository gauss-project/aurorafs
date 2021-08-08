// Code generated - DO NOT EDIT.
// This file is a generated binding and any manual changes will be lost.

package simpleswapfactory

import (
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
	_ = big.NewInt
	_ = strings.NewReader
	_ = ethereum.NotFound
	_ = bind.Bind
	_ = common.Big1
	_ = types.BloomLookup
	_ = event.NewSubscription
)

// AddressABI is the input ABI used to generate the binding from.
const AddressABI = "[]"

// AddressBin is the compiled bytecode used for deploying new contracts.
var AddressBin = "0x60566023600b82828239805160001a607314601657fe5b30600052607381538281f3fe73000000000000000000000000000000000000000030146080604052600080fdfea26469706673582212204881499fbb2fb42b69fde72ddfa541198f5f2d8bd8efa534186b507cca2c080664736f6c634300060c0033"

// DeployAddress deploys a new Ethereum contract, binding an instance of Address to it.
func DeployAddress(auth *bind.TransactOpts, backend bind.ContractBackend) (common.Address, *types.Transaction, *Address, error) {
	parsed, err := abi.JSON(strings.NewReader(AddressABI))
	if err != nil {
		return common.Address{}, nil, nil, err
	}

	address, tx, contract, err := bind.DeployContract(auth, parsed, common.FromHex(AddressBin), backend)
	if err != nil {
		return common.Address{}, nil, nil, err
	}
	return address, tx, &Address{AddressCaller: AddressCaller{contract: contract}, AddressTransactor: AddressTransactor{contract: contract}, AddressFilterer: AddressFilterer{contract: contract}}, nil
}

// Address is an auto generated Go binding around an Ethereum contract.
type Address struct {
	AddressCaller     // Read-only binding to the contract
	AddressTransactor // Write-only binding to the contract
	AddressFilterer   // Log filterer for contract events
}

// AddressCaller is an auto generated read-only Go binding around an Ethereum contract.
type AddressCaller struct {
	contract *bind.BoundContract // Generic contract wrapper for the low level calls
}

// AddressTransactor is an auto generated write-only Go binding around an Ethereum contract.
type AddressTransactor struct {
	contract *bind.BoundContract // Generic contract wrapper for the low level calls
}

// AddressFilterer is an auto generated log filtering Go binding around an Ethereum contract events.
type AddressFilterer struct {
	contract *bind.BoundContract // Generic contract wrapper for the low level calls
}

// AddressSession is an auto generated Go binding around an Ethereum contract,
// with pre-set call and transact options.
type AddressSession struct {
	Contract     *Address          // Generic contract binding to set the session for
	CallOpts     bind.CallOpts     // Call options to use throughout this session
	TransactOpts bind.TransactOpts // Transaction auth options to use throughout this session
}

// AddressCallerSession is an auto generated read-only Go binding around an Ethereum contract,
// with pre-set call options.
type AddressCallerSession struct {
	Contract *AddressCaller // Generic contract caller binding to set the session for
	CallOpts bind.CallOpts  // Call options to use throughout this session
}

// AddressTransactorSession is an auto generated write-only Go binding around an Ethereum contract,
// with pre-set transact options.
type AddressTransactorSession struct {
	Contract     *AddressTransactor // Generic contract transactor binding to set the session for
	TransactOpts bind.TransactOpts  // Transaction auth options to use throughout this session
}

// AddressRaw is an auto generated low-level Go binding around an Ethereum contract.
type AddressRaw struct {
	Contract *Address // Generic contract binding to access the raw methods on
}

// AddressCallerRaw is an auto generated low-level read-only Go binding around an Ethereum contract.
type AddressCallerRaw struct {
	Contract *AddressCaller // Generic read-only contract binding to access the raw methods on
}

// AddressTransactorRaw is an auto generated low-level write-only Go binding around an Ethereum contract.
type AddressTransactorRaw struct {
	Contract *AddressTransactor // Generic write-only contract binding to access the raw methods on
}

// NewAddress creates a new instance of Address, bound to a specific deployed contract.
func NewAddress(address common.Address, backend bind.ContractBackend) (*Address, error) {
	contract, err := bindAddress(address, backend, backend, backend)
	if err != nil {
		return nil, err
	}
	return &Address{AddressCaller: AddressCaller{contract: contract}, AddressTransactor: AddressTransactor{contract: contract}, AddressFilterer: AddressFilterer{contract: contract}}, nil
}

// NewAddressCaller creates a new read-only instance of Address, bound to a specific deployed contract.
func NewAddressCaller(address common.Address, caller bind.ContractCaller) (*AddressCaller, error) {
	contract, err := bindAddress(address, caller, nil, nil)
	if err != nil {
		return nil, err
	}
	return &AddressCaller{contract: contract}, nil
}

// NewAddressTransactor creates a new write-only instance of Address, bound to a specific deployed contract.
func NewAddressTransactor(address common.Address, transactor bind.ContractTransactor) (*AddressTransactor, error) {
	contract, err := bindAddress(address, nil, transactor, nil)
	if err != nil {
		return nil, err
	}
	return &AddressTransactor{contract: contract}, nil
}

// NewAddressFilterer creates a new log filterer instance of Address, bound to a specific deployed contract.
func NewAddressFilterer(address common.Address, filterer bind.ContractFilterer) (*AddressFilterer, error) {
	contract, err := bindAddress(address, nil, nil, filterer)
	if err != nil {
		return nil, err
	}
	return &AddressFilterer{contract: contract}, nil
}

// bindAddress binds a generic wrapper to an already deployed contract.
func bindAddress(address common.Address, caller bind.ContractCaller, transactor bind.ContractTransactor, filterer bind.ContractFilterer) (*bind.BoundContract, error) {
	parsed, err := abi.JSON(strings.NewReader(AddressABI))
	if err != nil {
		return nil, err
	}
	return bind.NewBoundContract(address, parsed, caller, transactor, filterer), nil
}

// Call invokes the (constant) contract method with params as input values and
// sets the output to result. The result type might be a single field for simple
// returns, a slice of interfaces for anonymous returns and a struct for named
// returns.
func (_Address *AddressRaw) Call(opts *bind.CallOpts, result *[]interface{}, method string, params ...interface{}) error {
	return _Address.Contract.AddressCaller.contract.Call(opts, result, method, params...)
}

// Transfer initiates a plain transaction to move funds to the contract, calling
// its default method if one is available.
func (_Address *AddressRaw) Transfer(opts *bind.TransactOpts) (*types.Transaction, error) {
	return _Address.Contract.AddressTransactor.contract.Transfer(opts)
}

// Transact invokes the (paid) contract method with params as input values.
func (_Address *AddressRaw) Transact(opts *bind.TransactOpts, method string, params ...interface{}) (*types.Transaction, error) {
	return _Address.Contract.AddressTransactor.contract.Transact(opts, method, params...)
}

// Call invokes the (constant) contract method with params as input values and
// sets the output to result. The result type might be a single field for simple
// returns, a slice of interfaces for anonymous returns and a struct for named
// returns.
func (_Address *AddressCallerRaw) Call(opts *bind.CallOpts, result *[]interface{}, method string, params ...interface{}) error {
	return _Address.Contract.contract.Call(opts, result, method, params...)
}

// Transfer initiates a plain transaction to move funds to the contract, calling
// its default method if one is available.
func (_Address *AddressTransactorRaw) Transfer(opts *bind.TransactOpts) (*types.Transaction, error) {
	return _Address.Contract.contract.Transfer(opts)
}

// Transact invokes the (paid) contract method with params as input values.
func (_Address *AddressTransactorRaw) Transact(opts *bind.TransactOpts, method string, params ...interface{}) (*types.Transaction, error) {
	return _Address.Contract.contract.Transact(opts, method, params...)
}

// ContextABI is the input ABI used to generate the binding from.
const ContextABI = "[]"

// Context is an auto generated Go binding around an Ethereum contract.
type Context struct {
	ContextCaller     // Read-only binding to the contract
	ContextTransactor // Write-only binding to the contract
	ContextFilterer   // Log filterer for contract events
}

// ContextCaller is an auto generated read-only Go binding around an Ethereum contract.
type ContextCaller struct {
	contract *bind.BoundContract // Generic contract wrapper for the low level calls
}

// ContextTransactor is an auto generated write-only Go binding around an Ethereum contract.
type ContextTransactor struct {
	contract *bind.BoundContract // Generic contract wrapper for the low level calls
}

// ContextFilterer is an auto generated log filtering Go binding around an Ethereum contract events.
type ContextFilterer struct {
	contract *bind.BoundContract // Generic contract wrapper for the low level calls
}

// ContextSession is an auto generated Go binding around an Ethereum contract,
// with pre-set call and transact options.
type ContextSession struct {
	Contract     *Context          // Generic contract binding to set the session for
	CallOpts     bind.CallOpts     // Call options to use throughout this session
	TransactOpts bind.TransactOpts // Transaction auth options to use throughout this session
}

// ContextCallerSession is an auto generated read-only Go binding around an Ethereum contract,
// with pre-set call options.
type ContextCallerSession struct {
	Contract *ContextCaller // Generic contract caller binding to set the session for
	CallOpts bind.CallOpts  // Call options to use throughout this session
}

// ContextTransactorSession is an auto generated write-only Go binding around an Ethereum contract,
// with pre-set transact options.
type ContextTransactorSession struct {
	Contract     *ContextTransactor // Generic contract transactor binding to set the session for
	TransactOpts bind.TransactOpts  // Transaction auth options to use throughout this session
}

// ContextRaw is an auto generated low-level Go binding around an Ethereum contract.
type ContextRaw struct {
	Contract *Context // Generic contract binding to access the raw methods on
}

// ContextCallerRaw is an auto generated low-level read-only Go binding around an Ethereum contract.
type ContextCallerRaw struct {
	Contract *ContextCaller // Generic read-only contract binding to access the raw methods on
}

// ContextTransactorRaw is an auto generated low-level write-only Go binding around an Ethereum contract.
type ContextTransactorRaw struct {
	Contract *ContextTransactor // Generic write-only contract binding to access the raw methods on
}

// NewContext creates a new instance of Context, bound to a specific deployed contract.
func NewContext(address common.Address, backend bind.ContractBackend) (*Context, error) {
	contract, err := bindContext(address, backend, backend, backend)
	if err != nil {
		return nil, err
	}
	return &Context{ContextCaller: ContextCaller{contract: contract}, ContextTransactor: ContextTransactor{contract: contract}, ContextFilterer: ContextFilterer{contract: contract}}, nil
}

// NewContextCaller creates a new read-only instance of Context, bound to a specific deployed contract.
func NewContextCaller(address common.Address, caller bind.ContractCaller) (*ContextCaller, error) {
	contract, err := bindContext(address, caller, nil, nil)
	if err != nil {
		return nil, err
	}
	return &ContextCaller{contract: contract}, nil
}

// NewContextTransactor creates a new write-only instance of Context, bound to a specific deployed contract.
func NewContextTransactor(address common.Address, transactor bind.ContractTransactor) (*ContextTransactor, error) {
	contract, err := bindContext(address, nil, transactor, nil)
	if err != nil {
		return nil, err
	}
	return &ContextTransactor{contract: contract}, nil
}

// NewContextFilterer creates a new log filterer instance of Context, bound to a specific deployed contract.
func NewContextFilterer(address common.Address, filterer bind.ContractFilterer) (*ContextFilterer, error) {
	contract, err := bindContext(address, nil, nil, filterer)
	if err != nil {
		return nil, err
	}
	return &ContextFilterer{contract: contract}, nil
}

// bindContext binds a generic wrapper to an already deployed contract.
func bindContext(address common.Address, caller bind.ContractCaller, transactor bind.ContractTransactor, filterer bind.ContractFilterer) (*bind.BoundContract, error) {
	parsed, err := abi.JSON(strings.NewReader(ContextABI))
	if err != nil {
		return nil, err
	}
	return bind.NewBoundContract(address, parsed, caller, transactor, filterer), nil
}

// Call invokes the (constant) contract method with params as input values and
// sets the output to result. The result type might be a single field for simple
// returns, a slice of interfaces for anonymous returns and a struct for named
// returns.
func (_Context *ContextRaw) Call(opts *bind.CallOpts, result *[]interface{}, method string, params ...interface{}) error {
	return _Context.Contract.ContextCaller.contract.Call(opts, result, method, params...)
}

// Transfer initiates a plain transaction to move funds to the contract, calling
// its default method if one is available.
func (_Context *ContextRaw) Transfer(opts *bind.TransactOpts) (*types.Transaction, error) {
	return _Context.Contract.ContextTransactor.contract.Transfer(opts)
}

// Transact invokes the (paid) contract method with params as input values.
func (_Context *ContextRaw) Transact(opts *bind.TransactOpts, method string, params ...interface{}) (*types.Transaction, error) {
	return _Context.Contract.ContextTransactor.contract.Transact(opts, method, params...)
}

// Call invokes the (constant) contract method with params as input values and
// sets the output to result. The result type might be a single field for simple
// returns, a slice of interfaces for anonymous returns and a struct for named
// returns.
func (_Context *ContextCallerRaw) Call(opts *bind.CallOpts, result *[]interface{}, method string, params ...interface{}) error {
	return _Context.Contract.contract.Call(opts, result, method, params...)
}

// Transfer initiates a plain transaction to move funds to the contract, calling
// its default method if one is available.
func (_Context *ContextTransactorRaw) Transfer(opts *bind.TransactOpts) (*types.Transaction, error) {
	return _Context.Contract.contract.Transfer(opts)
}

// Transact invokes the (paid) contract method with params as input values.
func (_Context *ContextTransactorRaw) Transact(opts *bind.TransactOpts, method string, params ...interface{}) (*types.Transaction, error) {
	return _Context.Contract.contract.Transact(opts, method, params...)
}

// ECDSAABI is the input ABI used to generate the binding from.
const ECDSAABI = "[]"

// ECDSABin is the compiled bytecode used for deploying new contracts.
var ECDSABin = "0x60566023600b82828239805160001a607314601657fe5b30600052607381538281f3fe73000000000000000000000000000000000000000030146080604052600080fdfea2646970667358221220bc437bed4aea56373f03528f4544a0535271d81bd62518394986032119ee6bb764736f6c634300060c0033"

// DeployECDSA deploys a new Ethereum contract, binding an instance of ECDSA to it.
func DeployECDSA(auth *bind.TransactOpts, backend bind.ContractBackend) (common.Address, *types.Transaction, *ECDSA, error) {
	parsed, err := abi.JSON(strings.NewReader(ECDSAABI))
	if err != nil {
		return common.Address{}, nil, nil, err
	}

	address, tx, contract, err := bind.DeployContract(auth, parsed, common.FromHex(ECDSABin), backend)
	if err != nil {
		return common.Address{}, nil, nil, err
	}
	return address, tx, &ECDSA{ECDSACaller: ECDSACaller{contract: contract}, ECDSATransactor: ECDSATransactor{contract: contract}, ECDSAFilterer: ECDSAFilterer{contract: contract}}, nil
}

// ECDSA is an auto generated Go binding around an Ethereum contract.
type ECDSA struct {
	ECDSACaller     // Read-only binding to the contract
	ECDSATransactor // Write-only binding to the contract
	ECDSAFilterer   // Log filterer for contract events
}

// ECDSACaller is an auto generated read-only Go binding around an Ethereum contract.
type ECDSACaller struct {
	contract *bind.BoundContract // Generic contract wrapper for the low level calls
}

// ECDSATransactor is an auto generated write-only Go binding around an Ethereum contract.
type ECDSATransactor struct {
	contract *bind.BoundContract // Generic contract wrapper for the low level calls
}

// ECDSAFilterer is an auto generated log filtering Go binding around an Ethereum contract events.
type ECDSAFilterer struct {
	contract *bind.BoundContract // Generic contract wrapper for the low level calls
}

// ECDSASession is an auto generated Go binding around an Ethereum contract,
// with pre-set call and transact options.
type ECDSASession struct {
	Contract     *ECDSA            // Generic contract binding to set the session for
	CallOpts     bind.CallOpts     // Call options to use throughout this session
	TransactOpts bind.TransactOpts // Transaction auth options to use throughout this session
}

// ECDSACallerSession is an auto generated read-only Go binding around an Ethereum contract,
// with pre-set call options.
type ECDSACallerSession struct {
	Contract *ECDSACaller  // Generic contract caller binding to set the session for
	CallOpts bind.CallOpts // Call options to use throughout this session
}

// ECDSATransactorSession is an auto generated write-only Go binding around an Ethereum contract,
// with pre-set transact options.
type ECDSATransactorSession struct {
	Contract     *ECDSATransactor  // Generic contract transactor binding to set the session for
	TransactOpts bind.TransactOpts // Transaction auth options to use throughout this session
}

// ECDSARaw is an auto generated low-level Go binding around an Ethereum contract.
type ECDSARaw struct {
	Contract *ECDSA // Generic contract binding to access the raw methods on
}

// ECDSACallerRaw is an auto generated low-level read-only Go binding around an Ethereum contract.
type ECDSACallerRaw struct {
	Contract *ECDSACaller // Generic read-only contract binding to access the raw methods on
}

// ECDSATransactorRaw is an auto generated low-level write-only Go binding around an Ethereum contract.
type ECDSATransactorRaw struct {
	Contract *ECDSATransactor // Generic write-only contract binding to access the raw methods on
}

// NewECDSA creates a new instance of ECDSA, bound to a specific deployed contract.
func NewECDSA(address common.Address, backend bind.ContractBackend) (*ECDSA, error) {
	contract, err := bindECDSA(address, backend, backend, backend)
	if err != nil {
		return nil, err
	}
	return &ECDSA{ECDSACaller: ECDSACaller{contract: contract}, ECDSATransactor: ECDSATransactor{contract: contract}, ECDSAFilterer: ECDSAFilterer{contract: contract}}, nil
}

// NewECDSACaller creates a new read-only instance of ECDSA, bound to a specific deployed contract.
func NewECDSACaller(address common.Address, caller bind.ContractCaller) (*ECDSACaller, error) {
	contract, err := bindECDSA(address, caller, nil, nil)
	if err != nil {
		return nil, err
	}
	return &ECDSACaller{contract: contract}, nil
}

// NewECDSATransactor creates a new write-only instance of ECDSA, bound to a specific deployed contract.
func NewECDSATransactor(address common.Address, transactor bind.ContractTransactor) (*ECDSATransactor, error) {
	contract, err := bindECDSA(address, nil, transactor, nil)
	if err != nil {
		return nil, err
	}
	return &ECDSATransactor{contract: contract}, nil
}

// NewECDSAFilterer creates a new log filterer instance of ECDSA, bound to a specific deployed contract.
func NewECDSAFilterer(address common.Address, filterer bind.ContractFilterer) (*ECDSAFilterer, error) {
	contract, err := bindECDSA(address, nil, nil, filterer)
	if err != nil {
		return nil, err
	}
	return &ECDSAFilterer{contract: contract}, nil
}

// bindECDSA binds a generic wrapper to an already deployed contract.
func bindECDSA(address common.Address, caller bind.ContractCaller, transactor bind.ContractTransactor, filterer bind.ContractFilterer) (*bind.BoundContract, error) {
	parsed, err := abi.JSON(strings.NewReader(ECDSAABI))
	if err != nil {
		return nil, err
	}
	return bind.NewBoundContract(address, parsed, caller, transactor, filterer), nil
}

// Call invokes the (constant) contract method with params as input values and
// sets the output to result. The result type might be a single field for simple
// returns, a slice of interfaces for anonymous returns and a struct for named
// returns.
func (_ECDSA *ECDSARaw) Call(opts *bind.CallOpts, result *[]interface{}, method string, params ...interface{}) error {
	return _ECDSA.Contract.ECDSACaller.contract.Call(opts, result, method, params...)
}

// Transfer initiates a plain transaction to move funds to the contract, calling
// its default method if one is available.
func (_ECDSA *ECDSARaw) Transfer(opts *bind.TransactOpts) (*types.Transaction, error) {
	return _ECDSA.Contract.ECDSATransactor.contract.Transfer(opts)
}

// Transact invokes the (paid) contract method with params as input values.
func (_ECDSA *ECDSARaw) Transact(opts *bind.TransactOpts, method string, params ...interface{}) (*types.Transaction, error) {
	return _ECDSA.Contract.ECDSATransactor.contract.Transact(opts, method, params...)
}

// Call invokes the (constant) contract method with params as input values and
// sets the output to result. The result type might be a single field for simple
// returns, a slice of interfaces for anonymous returns and a struct for named
// returns.
func (_ECDSA *ECDSACallerRaw) Call(opts *bind.CallOpts, result *[]interface{}, method string, params ...interface{}) error {
	return _ECDSA.Contract.contract.Call(opts, result, method, params...)
}

// Transfer initiates a plain transaction to move funds to the contract, calling
// its default method if one is available.
func (_ECDSA *ECDSATransactorRaw) Transfer(opts *bind.TransactOpts) (*types.Transaction, error) {
	return _ECDSA.Contract.contract.Transfer(opts)
}

// Transact invokes the (paid) contract method with params as input values.
func (_ECDSA *ECDSATransactorRaw) Transact(opts *bind.TransactOpts, method string, params ...interface{}) (*types.Transaction, error) {
	return _ECDSA.Contract.contract.Transact(opts, method, params...)
}

// ERC20ABI is the input ABI used to generate the binding from.
const ERC20ABI = "[{\"inputs\":[{\"internalType\":\"string\",\"name\":\"name\",\"type\":\"string\"},{\"internalType\":\"string\",\"name\":\"symbol\",\"type\":\"string\"}],\"stateMutability\":\"nonpayable\",\"type\":\"constructor\"},{\"anonymous\":false,\"inputs\":[{\"indexed\":true,\"internalType\":\"address\",\"name\":\"owner\",\"type\":\"address\"},{\"indexed\":true,\"internalType\":\"address\",\"name\":\"spender\",\"type\":\"address\"},{\"indexed\":false,\"internalType\":\"uint256\",\"name\":\"value\",\"type\":\"uint256\"}],\"name\":\"Approval\",\"type\":\"event\"},{\"anonymous\":false,\"inputs\":[{\"indexed\":true,\"internalType\":\"address\",\"name\":\"from\",\"type\":\"address\"},{\"indexed\":true,\"internalType\":\"address\",\"name\":\"to\",\"type\":\"address\"},{\"indexed\":false,\"internalType\":\"uint256\",\"name\":\"value\",\"type\":\"uint256\"}],\"name\":\"Transfer\",\"type\":\"event\"},{\"inputs\":[{\"internalType\":\"address\",\"name\":\"owner\",\"type\":\"address\"},{\"internalType\":\"address\",\"name\":\"spender\",\"type\":\"address\"}],\"name\":\"allowance\",\"outputs\":[{\"internalType\":\"uint256\",\"name\":\"\",\"type\":\"uint256\"}],\"stateMutability\":\"view\",\"type\":\"function\"},{\"inputs\":[{\"internalType\":\"address\",\"name\":\"spender\",\"type\":\"address\"},{\"internalType\":\"uint256\",\"name\":\"amount\",\"type\":\"uint256\"}],\"name\":\"approve\",\"outputs\":[{\"internalType\":\"bool\",\"name\":\"\",\"type\":\"bool\"}],\"stateMutability\":\"nonpayable\",\"type\":\"function\"},{\"inputs\":[{\"internalType\":\"address\",\"name\":\"account\",\"type\":\"address\"}],\"name\":\"balanceOf\",\"outputs\":[{\"internalType\":\"uint256\",\"name\":\"\",\"type\":\"uint256\"}],\"stateMutability\":\"view\",\"type\":\"function\"},{\"inputs\":[],\"name\":\"decimals\",\"outputs\":[{\"internalType\":\"uint8\",\"name\":\"\",\"type\":\"uint8\"}],\"stateMutability\":\"view\",\"type\":\"function\"},{\"inputs\":[{\"internalType\":\"address\",\"name\":\"spender\",\"type\":\"address\"},{\"internalType\":\"uint256\",\"name\":\"subtractedValue\",\"type\":\"uint256\"}],\"name\":\"decreaseAllowance\",\"outputs\":[{\"internalType\":\"bool\",\"name\":\"\",\"type\":\"bool\"}],\"stateMutability\":\"nonpayable\",\"type\":\"function\"},{\"inputs\":[{\"internalType\":\"address\",\"name\":\"spender\",\"type\":\"address\"},{\"internalType\":\"uint256\",\"name\":\"addedValue\",\"type\":\"uint256\"}],\"name\":\"increaseAllowance\",\"outputs\":[{\"internalType\":\"bool\",\"name\":\"\",\"type\":\"bool\"}],\"stateMutability\":\"nonpayable\",\"type\":\"function\"},{\"inputs\":[],\"name\":\"name\",\"outputs\":[{\"internalType\":\"string\",\"name\":\"\",\"type\":\"string\"}],\"stateMutability\":\"view\",\"type\":\"function\"},{\"inputs\":[],\"name\":\"symbol\",\"outputs\":[{\"internalType\":\"string\",\"name\":\"\",\"type\":\"string\"}],\"stateMutability\":\"view\",\"type\":\"function\"},{\"inputs\":[],\"name\":\"totalSupply\",\"outputs\":[{\"internalType\":\"uint256\",\"name\":\"\",\"type\":\"uint256\"}],\"stateMutability\":\"view\",\"type\":\"function\"},{\"inputs\":[{\"internalType\":\"address\",\"name\":\"recipient\",\"type\":\"address\"},{\"internalType\":\"uint256\",\"name\":\"amount\",\"type\":\"uint256\"}],\"name\":\"transfer\",\"outputs\":[{\"internalType\":\"bool\",\"name\":\"\",\"type\":\"bool\"}],\"stateMutability\":\"nonpayable\",\"type\":\"function\"},{\"inputs\":[{\"internalType\":\"address\",\"name\":\"sender\",\"type\":\"address\"},{\"internalType\":\"address\",\"name\":\"recipient\",\"type\":\"address\"},{\"internalType\":\"uint256\",\"name\":\"amount\",\"type\":\"uint256\"}],\"name\":\"transferFrom\",\"outputs\":[{\"internalType\":\"bool\",\"name\":\"\",\"type\":\"bool\"}],\"stateMutability\":\"nonpayable\",\"type\":\"function\"}]"

// ERC20Bin is the compiled bytecode used for deploying new contracts.
var ERC20Bin = "0x608060405234801561001057600080fd5b5060405162000c6238038062000c628339818101604052604081101561003557600080fd5b810190808051604051939291908464010000000082111561005557600080fd5b90830190602082018581111561006a57600080fd5b825164010000000081118282018810171561008457600080fd5b82525081516020918201929091019080838360005b838110156100b1578181015183820152602001610099565b50505050905090810190601f1680156100de5780820380516001836020036101000a031916815260200191505b506040526020018051604051939291908464010000000082111561010157600080fd5b90830190602082018581111561011657600080fd5b825164010000000081118282018810171561013057600080fd5b82525081516020918201929091019080838360005b8381101561015d578181015183820152602001610145565b50505050905090810190601f16801561018a5780820380516001836020036101000a031916815260200191505b50604052505082516101a4915060039060208501906101cd565b5080516101b89060049060208401906101cd565b50506005805460ff1916601217905550610260565b828054600181600116156101000203166002900490600052602060002090601f016020900481019282601f1061020e57805160ff191683800117855561023b565b8280016001018555821561023b579182015b8281111561023b578251825591602001919060010190610220565b5061024792915061024b565b5090565b5b80821115610247576000815560010161024c565b6109f280620002706000396000f3fe608060405234801561001057600080fd5b50600436106100a95760003560e01c8063395093511161007157806339509351146101d957806370a082311461020557806395d89b411461022b578063a457c2d714610233578063a9059cbb1461025f578063dd62ed3e1461028b576100a9565b806306fdde03146100ae578063095ea7b31461012b57806318160ddd1461016b57806323b872dd14610185578063313ce567146101bb575b600080fd5b6100b66102b9565b6040805160208082528351818301528351919283929083019185019080838360005b838110156100f05781810151838201526020016100d8565b50505050905090810190601f16801561011d5780820380516001836020036101000a031916815260200191505b509250505060405180910390f35b6101576004803603604081101561014157600080fd5b506001600160a01b03813516906020013561034f565b604080519115158252519081900360200190f35b61017361036c565b60408051918252519081900360200190f35b6101576004803603606081101561019b57600080fd5b506001600160a01b03813581169160208101359091169060400135610372565b6101c36103f9565b6040805160ff9092168252519081900360200190f35b610157600480360360408110156101ef57600080fd5b506001600160a01b038135169060200135610402565b6101736004803603602081101561021b57600080fd5b50356001600160a01b0316610450565b6100b661046b565b6101576004803603604081101561024957600080fd5b506001600160a01b0381351690602001356104cc565b6101576004803603604081101561027557600080fd5b506001600160a01b038135169060200135610534565b610173600480360360408110156102a157600080fd5b506001600160a01b0381358116916020013516610548565b60038054604080516020601f60026000196101006001881615020190951694909404938401819004810282018101909252828152606093909290918301828280156103455780601f1061031a57610100808354040283529160200191610345565b820191906000526020600020905b81548152906001019060200180831161032857829003601f168201915b5050505050905090565b600061036361035c610573565b8484610577565b50600192915050565b60025490565b600061037f848484610663565b6103ef8461038b610573565b6103ea85604051806060016040528060288152602001610927602891396001600160a01b038a166000908152600160205260408120906103c9610573565b6001600160a01b0316815260208101919091526040016000205491906107be565b610577565b5060019392505050565b60055460ff1690565b600061036361040f610573565b846103ea8560016000610420610573565b6001600160a01b03908116825260208083019390935260409182016000908120918c168152925290205490610855565b6001600160a01b031660009081526020819052604090205490565b60048054604080516020601f60026000196101006001881615020190951694909404938401819004810282018101909252828152606093909290918301828280156103455780601f1061031a57610100808354040283529160200191610345565b60006103636104d9610573565b846103ea856040518060600160405280602581526020016109986025913960016000610503610573565b6001600160a01b03908116825260208083019390935260409182016000908120918d168152925290205491906107be565b6000610363610541610573565b8484610663565b6001600160a01b03918216600090815260016020908152604080832093909416825291909152205490565b3390565b6001600160a01b0383166105bc5760405162461bcd60e51b81526004018080602001828103825260248152602001806109746024913960400191505060405180910390fd5b6001600160a01b0382166106015760405162461bcd60e51b81526004018080602001828103825260228152602001806108df6022913960400191505060405180910390fd5b6001600160a01b03808416600081815260016020908152604080832094871680845294825291829020859055815185815291517f8c5be1e5ebec7d5bd14f71427d1e84f3dd0314c0f7b2291e5b200ac8c7c3b9259281900390910190a3505050565b6001600160a01b0383166106a85760405162461bcd60e51b815260040180806020018281038252602581526020018061094f6025913960400191505060405180910390fd5b6001600160a01b0382166106ed5760405162461bcd60e51b81526004018080602001828103825260238152602001806108bc6023913960400191505060405180910390fd5b6106f88383836108b6565b61073581604051806060016040528060268152602001610901602691396001600160a01b03861660009081526020819052604090205491906107be565b6001600160a01b0380851660009081526020819052604080822093909355908416815220546107649082610855565b6001600160a01b038084166000818152602081815260409182902094909455805185815290519193928716927fddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef92918290030190a3505050565b6000818484111561084d5760405162461bcd60e51b81526004018080602001828103825283818151815260200191508051906020019080838360005b838110156108125781810151838201526020016107fa565b50505050905090810190601f16801561083f5780820380516001836020036101000a031916815260200191505b509250505060405180910390fd5b505050900390565b6000828201838110156108af576040805162461bcd60e51b815260206004820152601b60248201527f536166654d6174683a206164646974696f6e206f766572666c6f770000000000604482015290519081900360640190fd5b9392505050565b50505056fe45524332303a207472616e7366657220746f20746865207a65726f206164647265737345524332303a20617070726f766520746f20746865207a65726f206164647265737345524332303a207472616e7366657220616d6f756e7420657863656564732062616c616e636545524332303a207472616e7366657220616d6f756e74206578636565647320616c6c6f77616e636545524332303a207472616e736665722066726f6d20746865207a65726f206164647265737345524332303a20617070726f76652066726f6d20746865207a65726f206164647265737345524332303a2064656372656173656420616c6c6f77616e63652062656c6f77207a65726fa26469706673582212201cb8896fc83b3191ab71450c0a7ded11777813db76d961bcfe17ae56d864ebb464736f6c634300060c0033"

// DeployERC20 deploys a new Ethereum contract, binding an instance of ERC20 to it.
func DeployERC20(auth *bind.TransactOpts, backend bind.ContractBackend, name string, symbol string) (common.Address, *types.Transaction, *ERC20, error) {
	parsed, err := abi.JSON(strings.NewReader(ERC20ABI))
	if err != nil {
		return common.Address{}, nil, nil, err
	}

	address, tx, contract, err := bind.DeployContract(auth, parsed, common.FromHex(ERC20Bin), backend, name, symbol)
	if err != nil {
		return common.Address{}, nil, nil, err
	}
	return address, tx, &ERC20{ERC20Caller: ERC20Caller{contract: contract}, ERC20Transactor: ERC20Transactor{contract: contract}, ERC20Filterer: ERC20Filterer{contract: contract}}, nil
}

// ERC20 is an auto generated Go binding around an Ethereum contract.
type ERC20 struct {
	ERC20Caller     // Read-only binding to the contract
	ERC20Transactor // Write-only binding to the contract
	ERC20Filterer   // Log filterer for contract events
}

// ERC20Caller is an auto generated read-only Go binding around an Ethereum contract.
type ERC20Caller struct {
	contract *bind.BoundContract // Generic contract wrapper for the low level calls
}

// ERC20Transactor is an auto generated write-only Go binding around an Ethereum contract.
type ERC20Transactor struct {
	contract *bind.BoundContract // Generic contract wrapper for the low level calls
}

// ERC20Filterer is an auto generated log filtering Go binding around an Ethereum contract events.
type ERC20Filterer struct {
	contract *bind.BoundContract // Generic contract wrapper for the low level calls
}

// ERC20Session is an auto generated Go binding around an Ethereum contract,
// with pre-set call and transact options.
type ERC20Session struct {
	Contract     *ERC20            // Generic contract binding to set the session for
	CallOpts     bind.CallOpts     // Call options to use throughout this session
	TransactOpts bind.TransactOpts // Transaction auth options to use throughout this session
}

// ERC20CallerSession is an auto generated read-only Go binding around an Ethereum contract,
// with pre-set call options.
type ERC20CallerSession struct {
	Contract *ERC20Caller  // Generic contract caller binding to set the session for
	CallOpts bind.CallOpts // Call options to use throughout this session
}

// ERC20TransactorSession is an auto generated write-only Go binding around an Ethereum contract,
// with pre-set transact options.
type ERC20TransactorSession struct {
	Contract     *ERC20Transactor  // Generic contract transactor binding to set the session for
	TransactOpts bind.TransactOpts // Transaction auth options to use throughout this session
}

// ERC20Raw is an auto generated low-level Go binding around an Ethereum contract.
type ERC20Raw struct {
	Contract *ERC20 // Generic contract binding to access the raw methods on
}

// ERC20CallerRaw is an auto generated low-level read-only Go binding around an Ethereum contract.
type ERC20CallerRaw struct {
	Contract *ERC20Caller // Generic read-only contract binding to access the raw methods on
}

// ERC20TransactorRaw is an auto generated low-level write-only Go binding around an Ethereum contract.
type ERC20TransactorRaw struct {
	Contract *ERC20Transactor // Generic write-only contract binding to access the raw methods on
}

// NewERC20 creates a new instance of ERC20, bound to a specific deployed contract.
func NewERC20(address common.Address, backend bind.ContractBackend) (*ERC20, error) {
	contract, err := bindERC20(address, backend, backend, backend)
	if err != nil {
		return nil, err
	}
	return &ERC20{ERC20Caller: ERC20Caller{contract: contract}, ERC20Transactor: ERC20Transactor{contract: contract}, ERC20Filterer: ERC20Filterer{contract: contract}}, nil
}

// NewERC20Caller creates a new read-only instance of ERC20, bound to a specific deployed contract.
func NewERC20Caller(address common.Address, caller bind.ContractCaller) (*ERC20Caller, error) {
	contract, err := bindERC20(address, caller, nil, nil)
	if err != nil {
		return nil, err
	}
	return &ERC20Caller{contract: contract}, nil
}

// NewERC20Transactor creates a new write-only instance of ERC20, bound to a specific deployed contract.
func NewERC20Transactor(address common.Address, transactor bind.ContractTransactor) (*ERC20Transactor, error) {
	contract, err := bindERC20(address, nil, transactor, nil)
	if err != nil {
		return nil, err
	}
	return &ERC20Transactor{contract: contract}, nil
}

// NewERC20Filterer creates a new log filterer instance of ERC20, bound to a specific deployed contract.
func NewERC20Filterer(address common.Address, filterer bind.ContractFilterer) (*ERC20Filterer, error) {
	contract, err := bindERC20(address, nil, nil, filterer)
	if err != nil {
		return nil, err
	}
	return &ERC20Filterer{contract: contract}, nil
}

// bindERC20 binds a generic wrapper to an already deployed contract.
func bindERC20(address common.Address, caller bind.ContractCaller, transactor bind.ContractTransactor, filterer bind.ContractFilterer) (*bind.BoundContract, error) {
	parsed, err := abi.JSON(strings.NewReader(ERC20ABI))
	if err != nil {
		return nil, err
	}
	return bind.NewBoundContract(address, parsed, caller, transactor, filterer), nil
}

// Call invokes the (constant) contract method with params as input values and
// sets the output to result. The result type might be a single field for simple
// returns, a slice of interfaces for anonymous returns and a struct for named
// returns.
func (_ERC20 *ERC20Raw) Call(opts *bind.CallOpts, result *[]interface{}, method string, params ...interface{}) error {
	return _ERC20.Contract.ERC20Caller.contract.Call(opts, result, method, params...)
}

// Transfer initiates a plain transaction to move funds to the contract, calling
// its default method if one is available.
func (_ERC20 *ERC20Raw) Transfer(opts *bind.TransactOpts) (*types.Transaction, error) {
	return _ERC20.Contract.ERC20Transactor.contract.Transfer(opts)
}

// Transact invokes the (paid) contract method with params as input values.
func (_ERC20 *ERC20Raw) Transact(opts *bind.TransactOpts, method string, params ...interface{}) (*types.Transaction, error) {
	return _ERC20.Contract.ERC20Transactor.contract.Transact(opts, method, params...)
}

// Call invokes the (constant) contract method with params as input values and
// sets the output to result. The result type might be a single field for simple
// returns, a slice of interfaces for anonymous returns and a struct for named
// returns.
func (_ERC20 *ERC20CallerRaw) Call(opts *bind.CallOpts, result *[]interface{}, method string, params ...interface{}) error {
	return _ERC20.Contract.contract.Call(opts, result, method, params...)
}

// Transfer initiates a plain transaction to move funds to the contract, calling
// its default method if one is available.
func (_ERC20 *ERC20TransactorRaw) Transfer(opts *bind.TransactOpts) (*types.Transaction, error) {
	return _ERC20.Contract.contract.Transfer(opts)
}

// Transact invokes the (paid) contract method with params as input values.
func (_ERC20 *ERC20TransactorRaw) Transact(opts *bind.TransactOpts, method string, params ...interface{}) (*types.Transaction, error) {
	return _ERC20.Contract.contract.Transact(opts, method, params...)
}

// Allowance is a free data retrieval call binding the contract method 0xdd62ed3e.
//
// Solidity: function allowance(address owner, address spender) view returns(uint256)
func (_ERC20 *ERC20Caller) Allowance(opts *bind.CallOpts, owner common.Address, spender common.Address) (*big.Int, error) {
	var out []interface{}
	err := _ERC20.contract.Call(opts, &out, "allowance", owner, spender)

	if err != nil {
		return *new(*big.Int), err
	}

	out0 := *abi.ConvertType(out[0], new(*big.Int)).(**big.Int)

	return out0, err

}

// Allowance is a free data retrieval call binding the contract method 0xdd62ed3e.
//
// Solidity: function allowance(address owner, address spender) view returns(uint256)
func (_ERC20 *ERC20Session) Allowance(owner common.Address, spender common.Address) (*big.Int, error) {
	return _ERC20.Contract.Allowance(&_ERC20.CallOpts, owner, spender)
}

// Allowance is a free data retrieval call binding the contract method 0xdd62ed3e.
//
// Solidity: function allowance(address owner, address spender) view returns(uint256)
func (_ERC20 *ERC20CallerSession) Allowance(owner common.Address, spender common.Address) (*big.Int, error) {
	return _ERC20.Contract.Allowance(&_ERC20.CallOpts, owner, spender)
}

// BalanceOf is a free data retrieval call binding the contract method 0x70a08231.
//
// Solidity: function balanceOf(address account) view returns(uint256)
func (_ERC20 *ERC20Caller) BalanceOf(opts *bind.CallOpts, account common.Address) (*big.Int, error) {
	var out []interface{}
	err := _ERC20.contract.Call(opts, &out, "balanceOf", account)

	if err != nil {
		return *new(*big.Int), err
	}

	out0 := *abi.ConvertType(out[0], new(*big.Int)).(**big.Int)

	return out0, err

}

// BalanceOf is a free data retrieval call binding the contract method 0x70a08231.
//
// Solidity: function balanceOf(address account) view returns(uint256)
func (_ERC20 *ERC20Session) BalanceOf(account common.Address) (*big.Int, error) {
	return _ERC20.Contract.BalanceOf(&_ERC20.CallOpts, account)
}

// BalanceOf is a free data retrieval call binding the contract method 0x70a08231.
//
// Solidity: function balanceOf(address account) view returns(uint256)
func (_ERC20 *ERC20CallerSession) BalanceOf(account common.Address) (*big.Int, error) {
	return _ERC20.Contract.BalanceOf(&_ERC20.CallOpts, account)
}

// Decimals is a free data retrieval call binding the contract method 0x313ce567.
//
// Solidity: function decimals() view returns(uint8)
func (_ERC20 *ERC20Caller) Decimals(opts *bind.CallOpts) (uint8, error) {
	var out []interface{}
	err := _ERC20.contract.Call(opts, &out, "decimals")

	if err != nil {
		return *new(uint8), err
	}

	out0 := *abi.ConvertType(out[0], new(uint8)).(*uint8)

	return out0, err

}

// Decimals is a free data retrieval call binding the contract method 0x313ce567.
//
// Solidity: function decimals() view returns(uint8)
func (_ERC20 *ERC20Session) Decimals() (uint8, error) {
	return _ERC20.Contract.Decimals(&_ERC20.CallOpts)
}

// Decimals is a free data retrieval call binding the contract method 0x313ce567.
//
// Solidity: function decimals() view returns(uint8)
func (_ERC20 *ERC20CallerSession) Decimals() (uint8, error) {
	return _ERC20.Contract.Decimals(&_ERC20.CallOpts)
}

// Name is a free data retrieval call binding the contract method 0x06fdde03.
//
// Solidity: function name() view returns(string)
func (_ERC20 *ERC20Caller) Name(opts *bind.CallOpts) (string, error) {
	var out []interface{}
	err := _ERC20.contract.Call(opts, &out, "name")

	if err != nil {
		return *new(string), err
	}

	out0 := *abi.ConvertType(out[0], new(string)).(*string)

	return out0, err

}

// Name is a free data retrieval call binding the contract method 0x06fdde03.
//
// Solidity: function name() view returns(string)
func (_ERC20 *ERC20Session) Name() (string, error) {
	return _ERC20.Contract.Name(&_ERC20.CallOpts)
}

// Name is a free data retrieval call binding the contract method 0x06fdde03.
//
// Solidity: function name() view returns(string)
func (_ERC20 *ERC20CallerSession) Name() (string, error) {
	return _ERC20.Contract.Name(&_ERC20.CallOpts)
}

// Symbol is a free data retrieval call binding the contract method 0x95d89b41.
//
// Solidity: function symbol() view returns(string)
func (_ERC20 *ERC20Caller) Symbol(opts *bind.CallOpts) (string, error) {
	var out []interface{}
	err := _ERC20.contract.Call(opts, &out, "symbol")

	if err != nil {
		return *new(string), err
	}

	out0 := *abi.ConvertType(out[0], new(string)).(*string)

	return out0, err

}

// Symbol is a free data retrieval call binding the contract method 0x95d89b41.
//
// Solidity: function symbol() view returns(string)
func (_ERC20 *ERC20Session) Symbol() (string, error) {
	return _ERC20.Contract.Symbol(&_ERC20.CallOpts)
}

// Symbol is a free data retrieval call binding the contract method 0x95d89b41.
//
// Solidity: function symbol() view returns(string)
func (_ERC20 *ERC20CallerSession) Symbol() (string, error) {
	return _ERC20.Contract.Symbol(&_ERC20.CallOpts)
}

// TotalSupply is a free data retrieval call binding the contract method 0x18160ddd.
//
// Solidity: function totalSupply() view returns(uint256)
func (_ERC20 *ERC20Caller) TotalSupply(opts *bind.CallOpts) (*big.Int, error) {
	var out []interface{}
	err := _ERC20.contract.Call(opts, &out, "totalSupply")

	if err != nil {
		return *new(*big.Int), err
	}

	out0 := *abi.ConvertType(out[0], new(*big.Int)).(**big.Int)

	return out0, err

}

// TotalSupply is a free data retrieval call binding the contract method 0x18160ddd.
//
// Solidity: function totalSupply() view returns(uint256)
func (_ERC20 *ERC20Session) TotalSupply() (*big.Int, error) {
	return _ERC20.Contract.TotalSupply(&_ERC20.CallOpts)
}

// TotalSupply is a free data retrieval call binding the contract method 0x18160ddd.
//
// Solidity: function totalSupply() view returns(uint256)
func (_ERC20 *ERC20CallerSession) TotalSupply() (*big.Int, error) {
	return _ERC20.Contract.TotalSupply(&_ERC20.CallOpts)
}

// Approve is a paid mutator transaction binding the contract method 0x095ea7b3.
//
// Solidity: function approve(address spender, uint256 amount) returns(bool)
func (_ERC20 *ERC20Transactor) Approve(opts *bind.TransactOpts, spender common.Address, amount *big.Int) (*types.Transaction, error) {
	return _ERC20.contract.Transact(opts, "approve", spender, amount)
}

// Approve is a paid mutator transaction binding the contract method 0x095ea7b3.
//
// Solidity: function approve(address spender, uint256 amount) returns(bool)
func (_ERC20 *ERC20Session) Approve(spender common.Address, amount *big.Int) (*types.Transaction, error) {
	return _ERC20.Contract.Approve(&_ERC20.TransactOpts, spender, amount)
}

// Approve is a paid mutator transaction binding the contract method 0x095ea7b3.
//
// Solidity: function approve(address spender, uint256 amount) returns(bool)
func (_ERC20 *ERC20TransactorSession) Approve(spender common.Address, amount *big.Int) (*types.Transaction, error) {
	return _ERC20.Contract.Approve(&_ERC20.TransactOpts, spender, amount)
}

// DecreaseAllowance is a paid mutator transaction binding the contract method 0xa457c2d7.
//
// Solidity: function decreaseAllowance(address spender, uint256 subtractedValue) returns(bool)
func (_ERC20 *ERC20Transactor) DecreaseAllowance(opts *bind.TransactOpts, spender common.Address, subtractedValue *big.Int) (*types.Transaction, error) {
	return _ERC20.contract.Transact(opts, "decreaseAllowance", spender, subtractedValue)
}

// DecreaseAllowance is a paid mutator transaction binding the contract method 0xa457c2d7.
//
// Solidity: function decreaseAllowance(address spender, uint256 subtractedValue) returns(bool)
func (_ERC20 *ERC20Session) DecreaseAllowance(spender common.Address, subtractedValue *big.Int) (*types.Transaction, error) {
	return _ERC20.Contract.DecreaseAllowance(&_ERC20.TransactOpts, spender, subtractedValue)
}

// DecreaseAllowance is a paid mutator transaction binding the contract method 0xa457c2d7.
//
// Solidity: function decreaseAllowance(address spender, uint256 subtractedValue) returns(bool)
func (_ERC20 *ERC20TransactorSession) DecreaseAllowance(spender common.Address, subtractedValue *big.Int) (*types.Transaction, error) {
	return _ERC20.Contract.DecreaseAllowance(&_ERC20.TransactOpts, spender, subtractedValue)
}

// IncreaseAllowance is a paid mutator transaction binding the contract method 0x39509351.
//
// Solidity: function increaseAllowance(address spender, uint256 addedValue) returns(bool)
func (_ERC20 *ERC20Transactor) IncreaseAllowance(opts *bind.TransactOpts, spender common.Address, addedValue *big.Int) (*types.Transaction, error) {
	return _ERC20.contract.Transact(opts, "increaseAllowance", spender, addedValue)
}

// IncreaseAllowance is a paid mutator transaction binding the contract method 0x39509351.
//
// Solidity: function increaseAllowance(address spender, uint256 addedValue) returns(bool)
func (_ERC20 *ERC20Session) IncreaseAllowance(spender common.Address, addedValue *big.Int) (*types.Transaction, error) {
	return _ERC20.Contract.IncreaseAllowance(&_ERC20.TransactOpts, spender, addedValue)
}

// IncreaseAllowance is a paid mutator transaction binding the contract method 0x39509351.
//
// Solidity: function increaseAllowance(address spender, uint256 addedValue) returns(bool)
func (_ERC20 *ERC20TransactorSession) IncreaseAllowance(spender common.Address, addedValue *big.Int) (*types.Transaction, error) {
	return _ERC20.Contract.IncreaseAllowance(&_ERC20.TransactOpts, spender, addedValue)
}

// Transfer is a paid mutator transaction binding the contract method 0xa9059cbb.
//
// Solidity: function transfer(address recipient, uint256 amount) returns(bool)
func (_ERC20 *ERC20Transactor) Transfer(opts *bind.TransactOpts, recipient common.Address, amount *big.Int) (*types.Transaction, error) {
	return _ERC20.contract.Transact(opts, "transfer", recipient, amount)
}

// Transfer is a paid mutator transaction binding the contract method 0xa9059cbb.
//
// Solidity: function transfer(address recipient, uint256 amount) returns(bool)
func (_ERC20 *ERC20Session) Transfer(recipient common.Address, amount *big.Int) (*types.Transaction, error) {
	return _ERC20.Contract.Transfer(&_ERC20.TransactOpts, recipient, amount)
}

// Transfer is a paid mutator transaction binding the contract method 0xa9059cbb.
//
// Solidity: function transfer(address recipient, uint256 amount) returns(bool)
func (_ERC20 *ERC20TransactorSession) Transfer(recipient common.Address, amount *big.Int) (*types.Transaction, error) {
	return _ERC20.Contract.Transfer(&_ERC20.TransactOpts, recipient, amount)
}

// TransferFrom is a paid mutator transaction binding the contract method 0x23b872dd.
//
// Solidity: function transferFrom(address sender, address recipient, uint256 amount) returns(bool)
func (_ERC20 *ERC20Transactor) TransferFrom(opts *bind.TransactOpts, sender common.Address, recipient common.Address, amount *big.Int) (*types.Transaction, error) {
	return _ERC20.contract.Transact(opts, "transferFrom", sender, recipient, amount)
}

// TransferFrom is a paid mutator transaction binding the contract method 0x23b872dd.
//
// Solidity: function transferFrom(address sender, address recipient, uint256 amount) returns(bool)
func (_ERC20 *ERC20Session) TransferFrom(sender common.Address, recipient common.Address, amount *big.Int) (*types.Transaction, error) {
	return _ERC20.Contract.TransferFrom(&_ERC20.TransactOpts, sender, recipient, amount)
}

// TransferFrom is a paid mutator transaction binding the contract method 0x23b872dd.
//
// Solidity: function transferFrom(address sender, address recipient, uint256 amount) returns(bool)
func (_ERC20 *ERC20TransactorSession) TransferFrom(sender common.Address, recipient common.Address, amount *big.Int) (*types.Transaction, error) {
	return _ERC20.Contract.TransferFrom(&_ERC20.TransactOpts, sender, recipient, amount)
}

// ERC20ApprovalIterator is returned from FilterApproval and is used to iterate over the raw logs and unpacked data for Approval events raised by the ERC20 contract.
type ERC20ApprovalIterator struct {
	Event *ERC20Approval // Event containing the contract specifics and raw log

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
func (it *ERC20ApprovalIterator) Next() bool {
	// If the iterator failed, stop iterating
	if it.fail != nil {
		return false
	}
	// If the iterator completed, deliver directly whatever's available
	if it.done {
		select {
		case log := <-it.logs:
			it.Event = new(ERC20Approval)
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
		it.Event = new(ERC20Approval)
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
func (it *ERC20ApprovalIterator) Error() error {
	return it.fail
}

// Close terminates the iteration process, releasing any pending underlying
// resources.
func (it *ERC20ApprovalIterator) Close() error {
	it.sub.Unsubscribe()
	return nil
}

// ERC20Approval represents a Approval event raised by the ERC20 contract.
type ERC20Approval struct {
	Owner   common.Address
	Spender common.Address
	Value   *big.Int
	Raw     types.Log // Blockchain specific contextual infos
}

// FilterApproval is a free log retrieval operation binding the contract event 0x8c5be1e5ebec7d5bd14f71427d1e84f3dd0314c0f7b2291e5b200ac8c7c3b925.
//
// Solidity: event Approval(address indexed owner, address indexed spender, uint256 value)
func (_ERC20 *ERC20Filterer) FilterApproval(opts *bind.FilterOpts, owner []common.Address, spender []common.Address) (*ERC20ApprovalIterator, error) {

	var ownerRule []interface{}
	for _, ownerItem := range owner {
		ownerRule = append(ownerRule, ownerItem)
	}
	var spenderRule []interface{}
	for _, spenderItem := range spender {
		spenderRule = append(spenderRule, spenderItem)
	}

	logs, sub, err := _ERC20.contract.FilterLogs(opts, "Approval", ownerRule, spenderRule)
	if err != nil {
		return nil, err
	}
	return &ERC20ApprovalIterator{contract: _ERC20.contract, event: "Approval", logs: logs, sub: sub}, nil
}

// WatchApproval is a free log subscription operation binding the contract event 0x8c5be1e5ebec7d5bd14f71427d1e84f3dd0314c0f7b2291e5b200ac8c7c3b925.
//
// Solidity: event Approval(address indexed owner, address indexed spender, uint256 value)
func (_ERC20 *ERC20Filterer) WatchApproval(opts *bind.WatchOpts, sink chan<- *ERC20Approval, owner []common.Address, spender []common.Address) (event.Subscription, error) {

	var ownerRule []interface{}
	for _, ownerItem := range owner {
		ownerRule = append(ownerRule, ownerItem)
	}
	var spenderRule []interface{}
	for _, spenderItem := range spender {
		spenderRule = append(spenderRule, spenderItem)
	}

	logs, sub, err := _ERC20.contract.WatchLogs(opts, "Approval", ownerRule, spenderRule)
	if err != nil {
		return nil, err
	}
	return event.NewSubscription(func(quit <-chan struct{}) error {
		defer sub.Unsubscribe()
		for {
			select {
			case log := <-logs:
				// New log arrived, parse the event and forward to the user
				event := new(ERC20Approval)
				if err := _ERC20.contract.UnpackLog(event, "Approval", log); err != nil {
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
func (_ERC20 *ERC20Filterer) ParseApproval(log types.Log) (*ERC20Approval, error) {
	event := new(ERC20Approval)
	if err := _ERC20.contract.UnpackLog(event, "Approval", log); err != nil {
		return nil, err
	}
	event.Raw = log
	return event, nil
}

// ERC20TransferIterator is returned from FilterTransfer and is used to iterate over the raw logs and unpacked data for Transfer events raised by the ERC20 contract.
type ERC20TransferIterator struct {
	Event *ERC20Transfer // Event containing the contract specifics and raw log

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
func (it *ERC20TransferIterator) Next() bool {
	// If the iterator failed, stop iterating
	if it.fail != nil {
		return false
	}
	// If the iterator completed, deliver directly whatever's available
	if it.done {
		select {
		case log := <-it.logs:
			it.Event = new(ERC20Transfer)
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
		it.Event = new(ERC20Transfer)
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
func (it *ERC20TransferIterator) Error() error {
	return it.fail
}

// Close terminates the iteration process, releasing any pending underlying
// resources.
func (it *ERC20TransferIterator) Close() error {
	it.sub.Unsubscribe()
	return nil
}

// ERC20Transfer represents a Transfer event raised by the ERC20 contract.
type ERC20Transfer struct {
	From  common.Address
	To    common.Address
	Value *big.Int
	Raw   types.Log // Blockchain specific contextual infos
}

// FilterTransfer is a free log retrieval operation binding the contract event 0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef.
//
// Solidity: event Transfer(address indexed from, address indexed to, uint256 value)
func (_ERC20 *ERC20Filterer) FilterTransfer(opts *bind.FilterOpts, from []common.Address, to []common.Address) (*ERC20TransferIterator, error) {

	var fromRule []interface{}
	for _, fromItem := range from {
		fromRule = append(fromRule, fromItem)
	}
	var toRule []interface{}
	for _, toItem := range to {
		toRule = append(toRule, toItem)
	}

	logs, sub, err := _ERC20.contract.FilterLogs(opts, "Transfer", fromRule, toRule)
	if err != nil {
		return nil, err
	}
	return &ERC20TransferIterator{contract: _ERC20.contract, event: "Transfer", logs: logs, sub: sub}, nil
}

// WatchTransfer is a free log subscription operation binding the contract event 0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef.
//
// Solidity: event Transfer(address indexed from, address indexed to, uint256 value)
func (_ERC20 *ERC20Filterer) WatchTransfer(opts *bind.WatchOpts, sink chan<- *ERC20Transfer, from []common.Address, to []common.Address) (event.Subscription, error) {

	var fromRule []interface{}
	for _, fromItem := range from {
		fromRule = append(fromRule, fromItem)
	}
	var toRule []interface{}
	for _, toItem := range to {
		toRule = append(toRule, toItem)
	}

	logs, sub, err := _ERC20.contract.WatchLogs(opts, "Transfer", fromRule, toRule)
	if err != nil {
		return nil, err
	}
	return event.NewSubscription(func(quit <-chan struct{}) error {
		defer sub.Unsubscribe()
		for {
			select {
			case log := <-logs:
				// New log arrived, parse the event and forward to the user
				event := new(ERC20Transfer)
				if err := _ERC20.contract.UnpackLog(event, "Transfer", log); err != nil {
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
func (_ERC20 *ERC20Filterer) ParseTransfer(log types.Log) (*ERC20Transfer, error) {
	event := new(ERC20Transfer)
	if err := _ERC20.contract.UnpackLog(event, "Transfer", log); err != nil {
		return nil, err
	}
	event.Raw = log
	return event, nil
}

// ERC20SimpleSwapABI is the input ABI used to generate the binding from.
const ERC20SimpleSwapABI = "[{\"inputs\":[{\"internalType\":\"address\",\"name\":\"_issuer\",\"type\":\"address\"},{\"internalType\":\"address\",\"name\":\"_token\",\"type\":\"address\"},{\"internalType\":\"uint256\",\"name\":\"_defaultHardDepositTimeout\",\"type\":\"uint256\"}],\"stateMutability\":\"nonpayable\",\"type\":\"constructor\"},{\"anonymous\":false,\"inputs\":[],\"name\":\"ChequeBounced\",\"type\":\"event\"},{\"anonymous\":false,\"inputs\":[{\"indexed\":true,\"internalType\":\"address\",\"name\":\"beneficiary\",\"type\":\"address\"},{\"indexed\":true,\"internalType\":\"address\",\"name\":\"recipient\",\"type\":\"address\"},{\"indexed\":true,\"internalType\":\"address\",\"name\":\"caller\",\"type\":\"address\"},{\"indexed\":false,\"internalType\":\"uint256\",\"name\":\"totalPayout\",\"type\":\"uint256\"},{\"indexed\":false,\"internalType\":\"uint256\",\"name\":\"cumulativePayout\",\"type\":\"uint256\"},{\"indexed\":false,\"internalType\":\"uint256\",\"name\":\"callerPayout\",\"type\":\"uint256\"}],\"name\":\"ChequeCashed\",\"type\":\"event\"},{\"anonymous\":false,\"inputs\":[{\"indexed\":true,\"internalType\":\"address\",\"name\":\"beneficiary\",\"type\":\"address\"},{\"indexed\":false,\"internalType\":\"uint256\",\"name\":\"amount\",\"type\":\"uint256\"}],\"name\":\"HardDepositAmountChanged\",\"type\":\"event\"},{\"anonymous\":false,\"inputs\":[{\"indexed\":true,\"internalType\":\"address\",\"name\":\"beneficiary\",\"type\":\"address\"},{\"indexed\":false,\"internalType\":\"uint256\",\"name\":\"decreaseAmount\",\"type\":\"uint256\"}],\"name\":\"HardDepositDecreasePrepared\",\"type\":\"event\"},{\"anonymous\":false,\"inputs\":[{\"indexed\":true,\"internalType\":\"address\",\"name\":\"beneficiary\",\"type\":\"address\"},{\"indexed\":false,\"internalType\":\"uint256\",\"name\":\"timeout\",\"type\":\"uint256\"}],\"name\":\"HardDepositTimeoutChanged\",\"type\":\"event\"},{\"anonymous\":false,\"inputs\":[{\"indexed\":false,\"internalType\":\"uint256\",\"name\":\"amount\",\"type\":\"uint256\"}],\"name\":\"Withdraw\",\"type\":\"event\"},{\"inputs\":[],\"name\":\"CASHOUT_TYPEHASH\",\"outputs\":[{\"internalType\":\"bytes32\",\"name\":\"\",\"type\":\"bytes32\"}],\"stateMutability\":\"view\",\"type\":\"function\"},{\"inputs\":[],\"name\":\"CHEQUE_TYPEHASH\",\"outputs\":[{\"internalType\":\"bytes32\",\"name\":\"\",\"type\":\"bytes32\"}],\"stateMutability\":\"view\",\"type\":\"function\"},{\"inputs\":[],\"name\":\"CUSTOMDECREASETIMEOUT_TYPEHASH\",\"outputs\":[{\"internalType\":\"bytes32\",\"name\":\"\",\"type\":\"bytes32\"}],\"stateMutability\":\"view\",\"type\":\"function\"},{\"inputs\":[],\"name\":\"EIP712DOMAIN_TYPEHASH\",\"outputs\":[{\"internalType\":\"bytes32\",\"name\":\"\",\"type\":\"bytes32\"}],\"stateMutability\":\"view\",\"type\":\"function\"},{\"inputs\":[],\"name\":\"balance\",\"outputs\":[{\"internalType\":\"uint256\",\"name\":\"\",\"type\":\"uint256\"}],\"stateMutability\":\"view\",\"type\":\"function\"},{\"inputs\":[],\"name\":\"bounced\",\"outputs\":[{\"internalType\":\"bool\",\"name\":\"\",\"type\":\"bool\"}],\"stateMutability\":\"view\",\"type\":\"function\"},{\"inputs\":[{\"internalType\":\"address\",\"name\":\"beneficiary\",\"type\":\"address\"},{\"internalType\":\"address\",\"name\":\"recipient\",\"type\":\"address\"},{\"internalType\":\"uint256\",\"name\":\"cumulativePayout\",\"type\":\"uint256\"},{\"internalType\":\"bytes\",\"name\":\"beneficiarySig\",\"type\":\"bytes\"},{\"internalType\":\"uint256\",\"name\":\"callerPayout\",\"type\":\"uint256\"},{\"internalType\":\"bytes\",\"name\":\"issuerSig\",\"type\":\"bytes\"}],\"name\":\"cashCheque\",\"outputs\":[],\"stateMutability\":\"nonpayable\",\"type\":\"function\"},{\"inputs\":[{\"internalType\":\"address\",\"name\":\"recipient\",\"type\":\"address\"},{\"internalType\":\"uint256\",\"name\":\"cumulativePayout\",\"type\":\"uint256\"},{\"internalType\":\"bytes\",\"name\":\"issuerSig\",\"type\":\"bytes\"}],\"name\":\"cashChequeBeneficiary\",\"outputs\":[],\"stateMutability\":\"nonpayable\",\"type\":\"function\"},{\"inputs\":[{\"internalType\":\"address\",\"name\":\"beneficiary\",\"type\":\"address\"}],\"name\":\"decreaseHardDeposit\",\"outputs\":[],\"stateMutability\":\"nonpayable\",\"type\":\"function\"},{\"inputs\":[],\"name\":\"defaultHardDepositTimeout\",\"outputs\":[{\"internalType\":\"uint256\",\"name\":\"\",\"type\":\"uint256\"}],\"stateMutability\":\"view\",\"type\":\"function\"},{\"inputs\":[{\"internalType\":\"address\",\"name\":\"\",\"type\":\"address\"}],\"name\":\"hardDeposits\",\"outputs\":[{\"internalType\":\"uint256\",\"name\":\"amount\",\"type\":\"uint256\"},{\"internalType\":\"uint256\",\"name\":\"decreaseAmount\",\"type\":\"uint256\"},{\"internalType\":\"uint256\",\"name\":\"timeout\",\"type\":\"uint256\"},{\"internalType\":\"uint256\",\"name\":\"canBeDecreasedAt\",\"type\":\"uint256\"}],\"stateMutability\":\"view\",\"type\":\"function\"},{\"inputs\":[{\"internalType\":\"address\",\"name\":\"beneficiary\",\"type\":\"address\"},{\"internalType\":\"uint256\",\"name\":\"amount\",\"type\":\"uint256\"}],\"name\":\"increaseHardDeposit\",\"outputs\":[],\"stateMutability\":\"nonpayable\",\"type\":\"function\"},{\"inputs\":[],\"name\":\"issuer\",\"outputs\":[{\"internalType\":\"address\",\"name\":\"\",\"type\":\"address\"}],\"stateMutability\":\"view\",\"type\":\"function\"},{\"inputs\":[],\"name\":\"liquidBalance\",\"outputs\":[{\"internalType\":\"uint256\",\"name\":\"\",\"type\":\"uint256\"}],\"stateMutability\":\"view\",\"type\":\"function\"},{\"inputs\":[{\"internalType\":\"address\",\"name\":\"beneficiary\",\"type\":\"address\"}],\"name\":\"liquidBalanceFor\",\"outputs\":[{\"internalType\":\"uint256\",\"name\":\"\",\"type\":\"uint256\"}],\"stateMutability\":\"view\",\"type\":\"function\"},{\"inputs\":[{\"internalType\":\"address\",\"name\":\"\",\"type\":\"address\"}],\"name\":\"paidOut\",\"outputs\":[{\"internalType\":\"uint256\",\"name\":\"\",\"type\":\"uint256\"}],\"stateMutability\":\"view\",\"type\":\"function\"},{\"inputs\":[{\"internalType\":\"address\",\"name\":\"beneficiary\",\"type\":\"address\"},{\"internalType\":\"uint256\",\"name\":\"decreaseAmount\",\"type\":\"uint256\"}],\"name\":\"prepareDecreaseHardDeposit\",\"outputs\":[],\"stateMutability\":\"nonpayable\",\"type\":\"function\"},{\"inputs\":[{\"internalType\":\"address\",\"name\":\"beneficiary\",\"type\":\"address\"},{\"internalType\":\"uint256\",\"name\":\"hardDepositTimeout\",\"type\":\"uint256\"},{\"internalType\":\"bytes\",\"name\":\"beneficiarySig\",\"type\":\"bytes\"}],\"name\":\"setCustomHardDepositTimeout\",\"outputs\":[],\"stateMutability\":\"nonpayable\",\"type\":\"function\"},{\"inputs\":[],\"name\":\"token\",\"outputs\":[{\"internalType\":\"contractERC20\",\"name\":\"\",\"type\":\"address\"}],\"stateMutability\":\"view\",\"type\":\"function\"},{\"inputs\":[],\"name\":\"totalHardDeposit\",\"outputs\":[{\"internalType\":\"uint256\",\"name\":\"\",\"type\":\"uint256\"}],\"stateMutability\":\"view\",\"type\":\"function\"},{\"inputs\":[],\"name\":\"totalPaidOut\",\"outputs\":[{\"internalType\":\"uint256\",\"name\":\"\",\"type\":\"uint256\"}],\"stateMutability\":\"view\",\"type\":\"function\"},{\"inputs\":[{\"internalType\":\"uint256\",\"name\":\"amount\",\"type\":\"uint256\"}],\"name\":\"withdraw\",\"outputs\":[],\"stateMutability\":\"nonpayable\",\"type\":\"function\"}]"

// ERC20SimpleSwapBin is the compiled bytecode used for deploying new contracts.
var ERC20SimpleSwapBin = "0x608060405234801561001057600080fd5b506040516119b93803806119b98339818101604052606081101561003357600080fd5b5080516020820151604090920151600680546001600160a01b039384166001600160a01b0319918216179091556001805493909416921691909117909155600055611936806100836000396000f3fe608060405234801561001057600080fd5b50600436106101425760003560e01c8063b6343b0d116100b8578063b7ec1a331161007c578063b7ec1a33146104e2578063c49f91d3146104ea578063c76a4d31146104f2578063d4c9a8e814610518578063e0bcf13a146105d1578063fc0c546a146105d957610142565b8063b6343b0d1461043e578063b648b4171461048a578063b69ef8a8146104a6578063b7770350146104ae578063b7998907146104da57610142565b80631d1438481161010a5780631d1438481461037d5780632e1a7d4d146103a1578063338f3fed146103be578063488b017c146103ea57806381f03fcb146103f2578063946f46a21461041857610142565b80630d5f26591461014757806312101021146102025780631357e1dc1461021c57806315c3343f146102245780631633fb1d1461022c575b600080fd5b6102006004803603606081101561015d57600080fd5b6001600160a01b0382351691602081013591810190606081016040820135600160201b81111561018c57600080fd5b82018360208201111561019e57600080fd5b803590602001918460018302840111600160201b831117156101bf57600080fd5b91908080601f0160208091040260200160405190810160405280939291908181526020018383808284376000920191909152509295506105e1945050505050565b005b61020a6105f4565b60408051918252519081900360200190f35b61020a6105fa565b61020a610600565b610200600480360360c081101561024257600080fd5b6001600160a01b03823581169260208101359091169160408201359190810190608081016060820135600160201b81111561027c57600080fd5b82018360208201111561028e57600080fd5b803590602001918460018302840111600160201b831117156102af57600080fd5b91908080601f01602080910402602001604051908101604052809392919081815260200183838082843760009201919091525092958435959094909350604081019250602001359050600160201b81111561030957600080fd5b82018360208201111561031b57600080fd5b803590602001918460018302840111600160201b8311171561033c57600080fd5b91908080601f016020809104026020016040519081016040528093929190818152602001838380828437600092019190915250929550610624945050505050565b61038561069e565b604080516001600160a01b039092168252519081900360200190f35b610200600480360360208110156103b757600080fd5b50356106ad565b610200600480360360408110156103d457600080fd5b506001600160a01b03813516906020013561080e565b61020a61093a565b61020a6004803603602081101561040857600080fd5b50356001600160a01b031661095e565b6102006004803603602081101561042e57600080fd5b50356001600160a01b0316610970565b6104646004803603602081101561045457600080fd5b50356001600160a01b0316610a4b565b604080519485526020850193909352838301919091526060830152519081900360800190f35b610492610a72565b604080519115158252519081900360200190f35b61020a610a82565b610200600480360360408110156104c457600080fd5b506001600160a01b038135169060200135610afe565b61020a610c20565b61020a610c44565b61020a610c5f565b61020a6004803603602081101561050857600080fd5b50356001600160a01b0316610c83565b6102006004803603606081101561052e57600080fd5b6001600160a01b0382351691602081013591810190606081016040820135600160201b81111561055d57600080fd5b82018360208201111561056f57600080fd5b803590602001918460018302840111600160201b8311171561059057600080fd5b91908080601f016020809104026020016040519081016040528093929190818152602001838380828437600092019190915250929550610cb4945050505050565b61020a610dc7565b610385610dcd565b6105ef338484600085610ddc565b505050565b60005481565b60035481565b7f48ebe6deff4a5ee645c01506a026031e2a945d6f41f1f4e5098ad65347492c1281565b61063a61063430338789876111cb565b84611243565b6001600160a01b0316866001600160a01b0316146106895760405162461bcd60e51b81526004018080602001828103825260298152602001806118656029913960400191505060405180910390fd5b6106968686868585610ddc565b505050505050565b6006546001600160a01b031681565b6006546001600160a01b03163314610705576040805162461bcd60e51b815260206004820152601660248201527529b4b6b83632a9bbb0b81d103737ba1034b9b9bab2b960511b604482015290519081900360640190fd5b61070d610c44565b81111561074b5760405162461bcd60e51b81526004018080602001828103825260288152602001806118d96028913960400191505060405180910390fd5b6001546006546040805163a9059cbb60e01b81526001600160a01b039283166004820152602481018590529051919092169163a9059cbb9160448083019260209291908290030181600087803b1580156107a457600080fd5b505af11580156107b8573d6000803e3d6000fd5b505050506040513d60208110156107ce57600080fd5b505161080b5760405162461bcd60e51b815260040180806020018281038252602781526020018061183e6027913960400191505060405180910390fd5b50565b6006546001600160a01b03163314610866576040805162461bcd60e51b815260206004820152601660248201527529b4b6b83632a9bbb0b81d103737ba1034b9b9bab2b960511b604482015290519081900360640190fd5b61086e610a82565b60055461087b90836112a5565b11156108b85760405162461bcd60e51b81526004018080602001828103825260348152602001806117a16034913960400191505060405180910390fd5b6001600160a01b038216600090815260046020526040902080546108dc90836112a5565b81556005546108eb90836112a5565b60055560006003820155805460408051918252516001600160a01b038516917f2506c43272ded05d095b91dbba876e66e46888157d3e078db5691496e96c5fad919081900360200190a2505050565b7f7d824962dd0f01520922ea1766c987b1db570cd5db90bdba5ccf5e320607950281565b60026020526000908152604090205481565b6001600160a01b03811660009081526004602052604090206003810154421080159061099f5750600381015415155b6109da5760405162461bcd60e51b81526004018080602001828103825260258152602001806118196025913960400191505060405180910390fd5b600181015481546109ea91611306565b8155600060038201556001810154600554610a0491611306565b600555805460408051918252516001600160a01b038416917f2506c43272ded05d095b91dbba876e66e46888157d3e078db5691496e96c5fad919081900360200190a25050565b60046020526000908152604090208054600182015460028301546003909301549192909184565b600654600160a01b900460ff1681565b600154604080516370a0823160e01b815230600482015290516000926001600160a01b0316916370a08231916024808301926020929190829003018186803b158015610acd57600080fd5b505afa158015610ae1573d6000803e3d6000fd5b505050506040513d6020811015610af757600080fd5b5051905090565b6006546001600160a01b03163314610b56576040805162461bcd60e51b815260206004820152601660248201527529b4b6b83632a9bbb0b81d103737ba1034b9b9bab2b960511b604482015290519081900360640190fd5b6001600160a01b03821660009081526004602052604090208054821115610bae5760405162461bcd60e51b815260040180806020018281038252602781526020018061188e6027913960400191505060405180910390fd5b60008160020154600014610bc6578160020154610bca565b6000545b4281016003840155600183018490556040805185815290519192506001600160a01b038616917fc8305077b495025ec4c1d977b176a762c350bb18cad4666ce1ee85c32b78698a9181900360200190a250505050565b7fe95f353750f192082df064ca5142d3a2d6f0bef0f3ffad66d80d8af86b7a749a81565b6000610c5a600554610c54610a82565b90611306565b905090565b7fc2f8787176b8ac6bf7215b4adcc1e069bf4ab82d9ab1df05a57a91d425935b6e81565b6001600160a01b038116600090815260046020526040812054610cae90610ca8610c44565b906112a5565b92915050565b6006546001600160a01b03163314610d0c576040805162461bcd60e51b815260206004820152601660248201527529b4b6b83632a9bbb0b81d103737ba1034b9b9bab2b960511b604482015290519081900360640190fd5b610d20610d1a308585611348565b82611243565b6001600160a01b0316836001600160a01b031614610d6f5760405162461bcd60e51b81526004018080602001828103825260298152602001806118656029913960400191505060405180910390fd5b6001600160a01b038316600081815260046020908152604091829020600201859055815185815291517f7b816003a769eb718bd9c66bdbd2dd5827da3f92bc6645276876bd7957b08cf09281900390910190a2505050565b60055481565b6001546001600160a01b031681565b6006546001600160a01b03163314610e4857610dfc610d1a3087866113b1565b6006546001600160a01b03908116911614610e485760405162461bcd60e51b81526004018080602001828103825260248152602001806118b56024913960400191505060405180910390fd5b6001600160a01b038516600090815260026020526040812054610e6c908590611306565b90506000610e8282610e7d89610c83565b61141a565b6001600160a01b03881660009081526004602052604081205491925090610eaa90839061141a565b905084821015610f01576040805162461bcd60e51b815260206004820152601d60248201527f53696d706c65537761703a2063616e6e6f74207061792063616c6c6572000000604482015290519081900360640190fd5b8015610f54576001600160a01b038816600090815260046020526040902054610f2a9082611306565b6001600160a01b038916600090815260046020526040902055600554610f509082611306565b6005555b6001600160a01b038816600090815260026020526040902054610f7790836112a5565b6001600160a01b038916600090815260026020526040902055600354610f9d90836112a5565b6003556001546001600160a01b031663a9059cbb88610fbc8589611306565b6040518363ffffffff1660e01b815260040180836001600160a01b0316815260200182815260200192505050602060405180830381600087803b15801561100257600080fd5b505af1158015611016573d6000803e3d6000fd5b505050506040513d602081101561102c57600080fd5b50516110695760405162461bcd60e51b815260040180806020018281038252602781526020018061183e6027913960400191505060405180910390fd5b841561112a576001546040805163a9059cbb60e01b81523360048201526024810188905290516001600160a01b039092169163a9059cbb916044808201926020929091908290030181600087803b1580156110c357600080fd5b505af11580156110d7573d6000803e3d6000fd5b505050506040513d60208110156110ed57600080fd5b505161112a5760405162461bcd60e51b815260040180806020018281038252602781526020018061183e6027913960400191505060405180910390fd5b6040805183815260208101889052808201879052905133916001600160a01b038a811692908c16917f950494fc3642fae5221b6c32e0e45765c95ebb382a04a71b160db0843e74c99f919081900360600190a48183146111c1576006805460ff60a01b1916600160a01b1790556040517f3f4449c047e11092ec54dc0751b6b4817a9162745de856c893a26e611d18ffc490600090a15b5050505050505050565b604080517f7d824962dd0f01520922ea1766c987b1db570cd5db90bdba5ccf5e32060795026020808301919091526001600160a01b0397881682840152958716606082015260808101949094529190941660a083015260c0808301949094528051808303909401845260e09091019052815191012090565b600080611256611251611430565b61148a565b84604051602001808061190160f01b8152506002018381526020018281526020019250505060405160208183030381529060405280519060200120905061129d81846114fd565b949350505050565b6000828201838110156112ff576040805162461bcd60e51b815260206004820152601b60248201527f536166654d6174683a206164646974696f6e206f766572666c6f770000000000604482015290519081900360640190fd5b9392505050565b60006112ff83836040518060400160405280601e81526020017f536166654d6174683a207375627472616374696f6e206f766572666c6f7700008152506116e8565b604080517fe95f353750f192082df064ca5142d3a2d6f0bef0f3ffad66d80d8af86b7a749a6020808301919091526001600160a01b03958616828401529390941660608501526080808501929092528051808503909201825260a0909301909252815191012090565b604080517f48ebe6deff4a5ee645c01506a026031e2a945d6f41f1f4e5098ad65347492c126020808301919091526001600160a01b03958616828401529390941660608501526080808501929092528051808503909201825260a0909301909252815191012090565b600081831061142957816112ff565b5090919050565b61143861177f565b506040805160a081018252600a6060820190815269436865717565626f6f6b60b01b608083015281528151808301835260038152620312e360ec1b602082810191909152820152469181019190915290565b805180516020918201208183015180519083012060409384015184517fc2f8787176b8ac6bf7215b4adcc1e069bf4ab82d9ab1df05a57a91d425935b6e818601528086019390935260608301919091526080808301919091528351808303909101815260a0909101909252815191012090565b60008151604114611555576040805162461bcd60e51b815260206004820152601f60248201527f45434453413a20696e76616c6964207369676e6174757265206c656e67746800604482015290519081900360640190fd5b60208201516040830151606084015160001a7f7fffffffffffffffffffffffffffffff5d576e7357a4501ddfe92f46681b20a08211156115c65760405162461bcd60e51b81526004018080602001828103825260228152602001806117d56022913960400191505060405180910390fd5b8060ff16601b141580156115de57508060ff16601c14155b1561161a5760405162461bcd60e51b81526004018080602001828103825260228152602001806117f76022913960400191505060405180910390fd5b600060018783868660405160008152602001604052604051808581526020018460ff1681526020018381526020018281526020019450505050506020604051602081039080840390855afa158015611676573d6000803e3d6000fd5b5050604051601f1901519150506001600160a01b0381166116de576040805162461bcd60e51b815260206004820152601860248201527f45434453413a20696e76616c6964207369676e61747572650000000000000000604482015290519081900360640190fd5b9695505050505050565b600081848411156117775760405162461bcd60e51b81526004018080602001828103825283818151815260200191508051906020019080838360005b8381101561173c578181015183820152602001611724565b50505050905090810190601f1680156117695780820380516001836020036101000a031916815260200191505b509250505060405180910390fd5b505050900390565b6040518060600160405280606081526020016060815260200160008152509056fe53696d706c65537761703a2068617264206465706f7369742063616e6e6f74206265206d6f7265207468616e2062616c616e636545434453413a20696e76616c6964207369676e6174757265202773272076616c756545434453413a20696e76616c6964207369676e6174757265202776272076616c756553696d706c65537761703a206465706f736974206e6f74207965742074696d6564206f757453696d706c65537761703a2053696d706c65537761703a207472616e73666572206661696c656453696d706c65537761703a20696e76616c69642062656e6566696369617279207369676e617475726553696d706c65537761703a2068617264206465706f736974206e6f742073756666696369656e7453696d706c65537761703a20696e76616c696420697373756572207369676e617475726553696d706c65537761703a206c697175696442616c616e6365206e6f742073756666696369656e74a26469706673582212208a981966d096f1271490a970697020175e844416a120c1d26a5a037c4c131c0a64736f6c634300060c0033"

// DeployERC20SimpleSwap deploys a new Ethereum contract, binding an instance of ERC20SimpleSwap to it.
func DeployERC20SimpleSwap(auth *bind.TransactOpts, backend bind.ContractBackend, _issuer common.Address, _token common.Address, _defaultHardDepositTimeout *big.Int) (common.Address, *types.Transaction, *ERC20SimpleSwap, error) {
	parsed, err := abi.JSON(strings.NewReader(ERC20SimpleSwapABI))
	if err != nil {
		return common.Address{}, nil, nil, err
	}

	address, tx, contract, err := bind.DeployContract(auth, parsed, common.FromHex(ERC20SimpleSwapBin), backend, _issuer, _token, _defaultHardDepositTimeout)
	if err != nil {
		return common.Address{}, nil, nil, err
	}
	return address, tx, &ERC20SimpleSwap{ERC20SimpleSwapCaller: ERC20SimpleSwapCaller{contract: contract}, ERC20SimpleSwapTransactor: ERC20SimpleSwapTransactor{contract: contract}, ERC20SimpleSwapFilterer: ERC20SimpleSwapFilterer{contract: contract}}, nil
}

// ERC20SimpleSwap is an auto generated Go binding around an Ethereum contract.
type ERC20SimpleSwap struct {
	ERC20SimpleSwapCaller     // Read-only binding to the contract
	ERC20SimpleSwapTransactor // Write-only binding to the contract
	ERC20SimpleSwapFilterer   // Log filterer for contract events
}

// ERC20SimpleSwapCaller is an auto generated read-only Go binding around an Ethereum contract.
type ERC20SimpleSwapCaller struct {
	contract *bind.BoundContract // Generic contract wrapper for the low level calls
}

// ERC20SimpleSwapTransactor is an auto generated write-only Go binding around an Ethereum contract.
type ERC20SimpleSwapTransactor struct {
	contract *bind.BoundContract // Generic contract wrapper for the low level calls
}

// ERC20SimpleSwapFilterer is an auto generated log filtering Go binding around an Ethereum contract events.
type ERC20SimpleSwapFilterer struct {
	contract *bind.BoundContract // Generic contract wrapper for the low level calls
}

// ERC20SimpleSwapSession is an auto generated Go binding around an Ethereum contract,
// with pre-set call and transact options.
type ERC20SimpleSwapSession struct {
	Contract     *ERC20SimpleSwap  // Generic contract binding to set the session for
	CallOpts     bind.CallOpts     // Call options to use throughout this session
	TransactOpts bind.TransactOpts // Transaction auth options to use throughout this session
}

// ERC20SimpleSwapCallerSession is an auto generated read-only Go binding around an Ethereum contract,
// with pre-set call options.
type ERC20SimpleSwapCallerSession struct {
	Contract *ERC20SimpleSwapCaller // Generic contract caller binding to set the session for
	CallOpts bind.CallOpts          // Call options to use throughout this session
}

// ERC20SimpleSwapTransactorSession is an auto generated write-only Go binding around an Ethereum contract,
// with pre-set transact options.
type ERC20SimpleSwapTransactorSession struct {
	Contract     *ERC20SimpleSwapTransactor // Generic contract transactor binding to set the session for
	TransactOpts bind.TransactOpts          // Transaction auth options to use throughout this session
}

// ERC20SimpleSwapRaw is an auto generated low-level Go binding around an Ethereum contract.
type ERC20SimpleSwapRaw struct {
	Contract *ERC20SimpleSwap // Generic contract binding to access the raw methods on
}

// ERC20SimpleSwapCallerRaw is an auto generated low-level read-only Go binding around an Ethereum contract.
type ERC20SimpleSwapCallerRaw struct {
	Contract *ERC20SimpleSwapCaller // Generic read-only contract binding to access the raw methods on
}

// ERC20SimpleSwapTransactorRaw is an auto generated low-level write-only Go binding around an Ethereum contract.
type ERC20SimpleSwapTransactorRaw struct {
	Contract *ERC20SimpleSwapTransactor // Generic write-only contract binding to access the raw methods on
}

// NewERC20SimpleSwap creates a new instance of ERC20SimpleSwap, bound to a specific deployed contract.
func NewERC20SimpleSwap(address common.Address, backend bind.ContractBackend) (*ERC20SimpleSwap, error) {
	contract, err := bindERC20SimpleSwap(address, backend, backend, backend)
	if err != nil {
		return nil, err
	}
	return &ERC20SimpleSwap{ERC20SimpleSwapCaller: ERC20SimpleSwapCaller{contract: contract}, ERC20SimpleSwapTransactor: ERC20SimpleSwapTransactor{contract: contract}, ERC20SimpleSwapFilterer: ERC20SimpleSwapFilterer{contract: contract}}, nil
}

// NewERC20SimpleSwapCaller creates a new read-only instance of ERC20SimpleSwap, bound to a specific deployed contract.
func NewERC20SimpleSwapCaller(address common.Address, caller bind.ContractCaller) (*ERC20SimpleSwapCaller, error) {
	contract, err := bindERC20SimpleSwap(address, caller, nil, nil)
	if err != nil {
		return nil, err
	}
	return &ERC20SimpleSwapCaller{contract: contract}, nil
}

// NewERC20SimpleSwapTransactor creates a new write-only instance of ERC20SimpleSwap, bound to a specific deployed contract.
func NewERC20SimpleSwapTransactor(address common.Address, transactor bind.ContractTransactor) (*ERC20SimpleSwapTransactor, error) {
	contract, err := bindERC20SimpleSwap(address, nil, transactor, nil)
	if err != nil {
		return nil, err
	}
	return &ERC20SimpleSwapTransactor{contract: contract}, nil
}

// NewERC20SimpleSwapFilterer creates a new log filterer instance of ERC20SimpleSwap, bound to a specific deployed contract.
func NewERC20SimpleSwapFilterer(address common.Address, filterer bind.ContractFilterer) (*ERC20SimpleSwapFilterer, error) {
	contract, err := bindERC20SimpleSwap(address, nil, nil, filterer)
	if err != nil {
		return nil, err
	}
	return &ERC20SimpleSwapFilterer{contract: contract}, nil
}

// bindERC20SimpleSwap binds a generic wrapper to an already deployed contract.
func bindERC20SimpleSwap(address common.Address, caller bind.ContractCaller, transactor bind.ContractTransactor, filterer bind.ContractFilterer) (*bind.BoundContract, error) {
	parsed, err := abi.JSON(strings.NewReader(ERC20SimpleSwapABI))
	if err != nil {
		return nil, err
	}
	return bind.NewBoundContract(address, parsed, caller, transactor, filterer), nil
}

// Call invokes the (constant) contract method with params as input values and
// sets the output to result. The result type might be a single field for simple
// returns, a slice of interfaces for anonymous returns and a struct for named
// returns.
func (_ERC20SimpleSwap *ERC20SimpleSwapRaw) Call(opts *bind.CallOpts, result *[]interface{}, method string, params ...interface{}) error {
	return _ERC20SimpleSwap.Contract.ERC20SimpleSwapCaller.contract.Call(opts, result, method, params...)
}

// Transfer initiates a plain transaction to move funds to the contract, calling
// its default method if one is available.
func (_ERC20SimpleSwap *ERC20SimpleSwapRaw) Transfer(opts *bind.TransactOpts) (*types.Transaction, error) {
	return _ERC20SimpleSwap.Contract.ERC20SimpleSwapTransactor.contract.Transfer(opts)
}

// Transact invokes the (paid) contract method with params as input values.
func (_ERC20SimpleSwap *ERC20SimpleSwapRaw) Transact(opts *bind.TransactOpts, method string, params ...interface{}) (*types.Transaction, error) {
	return _ERC20SimpleSwap.Contract.ERC20SimpleSwapTransactor.contract.Transact(opts, method, params...)
}

// Call invokes the (constant) contract method with params as input values and
// sets the output to result. The result type might be a single field for simple
// returns, a slice of interfaces for anonymous returns and a struct for named
// returns.
func (_ERC20SimpleSwap *ERC20SimpleSwapCallerRaw) Call(opts *bind.CallOpts, result *[]interface{}, method string, params ...interface{}) error {
	return _ERC20SimpleSwap.Contract.contract.Call(opts, result, method, params...)
}

// Transfer initiates a plain transaction to move funds to the contract, calling
// its default method if one is available.
func (_ERC20SimpleSwap *ERC20SimpleSwapTransactorRaw) Transfer(opts *bind.TransactOpts) (*types.Transaction, error) {
	return _ERC20SimpleSwap.Contract.contract.Transfer(opts)
}

// Transact invokes the (paid) contract method with params as input values.
func (_ERC20SimpleSwap *ERC20SimpleSwapTransactorRaw) Transact(opts *bind.TransactOpts, method string, params ...interface{}) (*types.Transaction, error) {
	return _ERC20SimpleSwap.Contract.contract.Transact(opts, method, params...)
}

// CASHOUTTYPEHASH is a free data retrieval call binding the contract method 0x488b017c.
//
// Solidity: function CASHOUT_TYPEHASH() view returns(bytes32)
func (_ERC20SimpleSwap *ERC20SimpleSwapCaller) CASHOUTTYPEHASH(opts *bind.CallOpts) ([32]byte, error) {
	var out []interface{}
	err := _ERC20SimpleSwap.contract.Call(opts, &out, "CASHOUT_TYPEHASH")

	if err != nil {
		return *new([32]byte), err
	}

	out0 := *abi.ConvertType(out[0], new([32]byte)).(*[32]byte)

	return out0, err

}

// CASHOUTTYPEHASH is a free data retrieval call binding the contract method 0x488b017c.
//
// Solidity: function CASHOUT_TYPEHASH() view returns(bytes32)
func (_ERC20SimpleSwap *ERC20SimpleSwapSession) CASHOUTTYPEHASH() ([32]byte, error) {
	return _ERC20SimpleSwap.Contract.CASHOUTTYPEHASH(&_ERC20SimpleSwap.CallOpts)
}

// CASHOUTTYPEHASH is a free data retrieval call binding the contract method 0x488b017c.
//
// Solidity: function CASHOUT_TYPEHASH() view returns(bytes32)
func (_ERC20SimpleSwap *ERC20SimpleSwapCallerSession) CASHOUTTYPEHASH() ([32]byte, error) {
	return _ERC20SimpleSwap.Contract.CASHOUTTYPEHASH(&_ERC20SimpleSwap.CallOpts)
}

// CHEQUETYPEHASH is a free data retrieval call binding the contract method 0x15c3343f.
//
// Solidity: function CHEQUE_TYPEHASH() view returns(bytes32)
func (_ERC20SimpleSwap *ERC20SimpleSwapCaller) CHEQUETYPEHASH(opts *bind.CallOpts) ([32]byte, error) {
	var out []interface{}
	err := _ERC20SimpleSwap.contract.Call(opts, &out, "CHEQUE_TYPEHASH")

	if err != nil {
		return *new([32]byte), err
	}

	out0 := *abi.ConvertType(out[0], new([32]byte)).(*[32]byte)

	return out0, err

}

// CHEQUETYPEHASH is a free data retrieval call binding the contract method 0x15c3343f.
//
// Solidity: function CHEQUE_TYPEHASH() view returns(bytes32)
func (_ERC20SimpleSwap *ERC20SimpleSwapSession) CHEQUETYPEHASH() ([32]byte, error) {
	return _ERC20SimpleSwap.Contract.CHEQUETYPEHASH(&_ERC20SimpleSwap.CallOpts)
}

// CHEQUETYPEHASH is a free data retrieval call binding the contract method 0x15c3343f.
//
// Solidity: function CHEQUE_TYPEHASH() view returns(bytes32)
func (_ERC20SimpleSwap *ERC20SimpleSwapCallerSession) CHEQUETYPEHASH() ([32]byte, error) {
	return _ERC20SimpleSwap.Contract.CHEQUETYPEHASH(&_ERC20SimpleSwap.CallOpts)
}

// CUSTOMDECREASETIMEOUTTYPEHASH is a free data retrieval call binding the contract method 0xb7998907.
//
// Solidity: function CUSTOMDECREASETIMEOUT_TYPEHASH() view returns(bytes32)
func (_ERC20SimpleSwap *ERC20SimpleSwapCaller) CUSTOMDECREASETIMEOUTTYPEHASH(opts *bind.CallOpts) ([32]byte, error) {
	var out []interface{}
	err := _ERC20SimpleSwap.contract.Call(opts, &out, "CUSTOMDECREASETIMEOUT_TYPEHASH")

	if err != nil {
		return *new([32]byte), err
	}

	out0 := *abi.ConvertType(out[0], new([32]byte)).(*[32]byte)

	return out0, err

}

// CUSTOMDECREASETIMEOUTTYPEHASH is a free data retrieval call binding the contract method 0xb7998907.
//
// Solidity: function CUSTOMDECREASETIMEOUT_TYPEHASH() view returns(bytes32)
func (_ERC20SimpleSwap *ERC20SimpleSwapSession) CUSTOMDECREASETIMEOUTTYPEHASH() ([32]byte, error) {
	return _ERC20SimpleSwap.Contract.CUSTOMDECREASETIMEOUTTYPEHASH(&_ERC20SimpleSwap.CallOpts)
}

// CUSTOMDECREASETIMEOUTTYPEHASH is a free data retrieval call binding the contract method 0xb7998907.
//
// Solidity: function CUSTOMDECREASETIMEOUT_TYPEHASH() view returns(bytes32)
func (_ERC20SimpleSwap *ERC20SimpleSwapCallerSession) CUSTOMDECREASETIMEOUTTYPEHASH() ([32]byte, error) {
	return _ERC20SimpleSwap.Contract.CUSTOMDECREASETIMEOUTTYPEHASH(&_ERC20SimpleSwap.CallOpts)
}

// EIP712DOMAINTYPEHASH is a free data retrieval call binding the contract method 0xc49f91d3.
//
// Solidity: function EIP712DOMAIN_TYPEHASH() view returns(bytes32)
func (_ERC20SimpleSwap *ERC20SimpleSwapCaller) EIP712DOMAINTYPEHASH(opts *bind.CallOpts) ([32]byte, error) {
	var out []interface{}
	err := _ERC20SimpleSwap.contract.Call(opts, &out, "EIP712DOMAIN_TYPEHASH")

	if err != nil {
		return *new([32]byte), err
	}

	out0 := *abi.ConvertType(out[0], new([32]byte)).(*[32]byte)

	return out0, err

}

// EIP712DOMAINTYPEHASH is a free data retrieval call binding the contract method 0xc49f91d3.
//
// Solidity: function EIP712DOMAIN_TYPEHASH() view returns(bytes32)
func (_ERC20SimpleSwap *ERC20SimpleSwapSession) EIP712DOMAINTYPEHASH() ([32]byte, error) {
	return _ERC20SimpleSwap.Contract.EIP712DOMAINTYPEHASH(&_ERC20SimpleSwap.CallOpts)
}

// EIP712DOMAINTYPEHASH is a free data retrieval call binding the contract method 0xc49f91d3.
//
// Solidity: function EIP712DOMAIN_TYPEHASH() view returns(bytes32)
func (_ERC20SimpleSwap *ERC20SimpleSwapCallerSession) EIP712DOMAINTYPEHASH() ([32]byte, error) {
	return _ERC20SimpleSwap.Contract.EIP712DOMAINTYPEHASH(&_ERC20SimpleSwap.CallOpts)
}

// Balance is a free data retrieval call binding the contract method 0xb69ef8a8.
//
// Solidity: function balance() view returns(uint256)
func (_ERC20SimpleSwap *ERC20SimpleSwapCaller) Balance(opts *bind.CallOpts) (*big.Int, error) {
	var out []interface{}
	err := _ERC20SimpleSwap.contract.Call(opts, &out, "balance")

	if err != nil {
		return *new(*big.Int), err
	}

	out0 := *abi.ConvertType(out[0], new(*big.Int)).(**big.Int)

	return out0, err

}

// Balance is a free data retrieval call binding the contract method 0xb69ef8a8.
//
// Solidity: function balance() view returns(uint256)
func (_ERC20SimpleSwap *ERC20SimpleSwapSession) Balance() (*big.Int, error) {
	return _ERC20SimpleSwap.Contract.Balance(&_ERC20SimpleSwap.CallOpts)
}

// Balance is a free data retrieval call binding the contract method 0xb69ef8a8.
//
// Solidity: function balance() view returns(uint256)
func (_ERC20SimpleSwap *ERC20SimpleSwapCallerSession) Balance() (*big.Int, error) {
	return _ERC20SimpleSwap.Contract.Balance(&_ERC20SimpleSwap.CallOpts)
}

// Bounced is a free data retrieval call binding the contract method 0xb648b417.
//
// Solidity: function bounced() view returns(bool)
func (_ERC20SimpleSwap *ERC20SimpleSwapCaller) Bounced(opts *bind.CallOpts) (bool, error) {
	var out []interface{}
	err := _ERC20SimpleSwap.contract.Call(opts, &out, "bounced")

	if err != nil {
		return *new(bool), err
	}

	out0 := *abi.ConvertType(out[0], new(bool)).(*bool)

	return out0, err

}

// Bounced is a free data retrieval call binding the contract method 0xb648b417.
//
// Solidity: function bounced() view returns(bool)
func (_ERC20SimpleSwap *ERC20SimpleSwapSession) Bounced() (bool, error) {
	return _ERC20SimpleSwap.Contract.Bounced(&_ERC20SimpleSwap.CallOpts)
}

// Bounced is a free data retrieval call binding the contract method 0xb648b417.
//
// Solidity: function bounced() view returns(bool)
func (_ERC20SimpleSwap *ERC20SimpleSwapCallerSession) Bounced() (bool, error) {
	return _ERC20SimpleSwap.Contract.Bounced(&_ERC20SimpleSwap.CallOpts)
}

// DefaultHardDepositTimeout is a free data retrieval call binding the contract method 0x12101021.
//
// Solidity: function defaultHardDepositTimeout() view returns(uint256)
func (_ERC20SimpleSwap *ERC20SimpleSwapCaller) DefaultHardDepositTimeout(opts *bind.CallOpts) (*big.Int, error) {
	var out []interface{}
	err := _ERC20SimpleSwap.contract.Call(opts, &out, "defaultHardDepositTimeout")

	if err != nil {
		return *new(*big.Int), err
	}

	out0 := *abi.ConvertType(out[0], new(*big.Int)).(**big.Int)

	return out0, err

}

// DefaultHardDepositTimeout is a free data retrieval call binding the contract method 0x12101021.
//
// Solidity: function defaultHardDepositTimeout() view returns(uint256)
func (_ERC20SimpleSwap *ERC20SimpleSwapSession) DefaultHardDepositTimeout() (*big.Int, error) {
	return _ERC20SimpleSwap.Contract.DefaultHardDepositTimeout(&_ERC20SimpleSwap.CallOpts)
}

// DefaultHardDepositTimeout is a free data retrieval call binding the contract method 0x12101021.
//
// Solidity: function defaultHardDepositTimeout() view returns(uint256)
func (_ERC20SimpleSwap *ERC20SimpleSwapCallerSession) DefaultHardDepositTimeout() (*big.Int, error) {
	return _ERC20SimpleSwap.Contract.DefaultHardDepositTimeout(&_ERC20SimpleSwap.CallOpts)
}

// HardDeposits is a free data retrieval call binding the contract method 0xb6343b0d.
//
// Solidity: function hardDeposits(address ) view returns(uint256 amount, uint256 decreaseAmount, uint256 timeout, uint256 canBeDecreasedAt)
func (_ERC20SimpleSwap *ERC20SimpleSwapCaller) HardDeposits(opts *bind.CallOpts, arg0 common.Address) (struct {
	Amount           *big.Int
	DecreaseAmount   *big.Int
	Timeout          *big.Int
	CanBeDecreasedAt *big.Int
}, error) {
	var out []interface{}
	err := _ERC20SimpleSwap.contract.Call(opts, &out, "hardDeposits", arg0)

	outstruct := new(struct {
		Amount           *big.Int
		DecreaseAmount   *big.Int
		Timeout          *big.Int
		CanBeDecreasedAt *big.Int
	})
	if err != nil {
		return *outstruct, err
	}

	outstruct.Amount = *abi.ConvertType(out[0], new(*big.Int)).(**big.Int)
	outstruct.DecreaseAmount = *abi.ConvertType(out[1], new(*big.Int)).(**big.Int)
	outstruct.Timeout = *abi.ConvertType(out[2], new(*big.Int)).(**big.Int)
	outstruct.CanBeDecreasedAt = *abi.ConvertType(out[3], new(*big.Int)).(**big.Int)

	return *outstruct, err

}

// HardDeposits is a free data retrieval call binding the contract method 0xb6343b0d.
//
// Solidity: function hardDeposits(address ) view returns(uint256 amount, uint256 decreaseAmount, uint256 timeout, uint256 canBeDecreasedAt)
func (_ERC20SimpleSwap *ERC20SimpleSwapSession) HardDeposits(arg0 common.Address) (struct {
	Amount           *big.Int
	DecreaseAmount   *big.Int
	Timeout          *big.Int
	CanBeDecreasedAt *big.Int
}, error) {
	return _ERC20SimpleSwap.Contract.HardDeposits(&_ERC20SimpleSwap.CallOpts, arg0)
}

// HardDeposits is a free data retrieval call binding the contract method 0xb6343b0d.
//
// Solidity: function hardDeposits(address ) view returns(uint256 amount, uint256 decreaseAmount, uint256 timeout, uint256 canBeDecreasedAt)
func (_ERC20SimpleSwap *ERC20SimpleSwapCallerSession) HardDeposits(arg0 common.Address) (struct {
	Amount           *big.Int
	DecreaseAmount   *big.Int
	Timeout          *big.Int
	CanBeDecreasedAt *big.Int
}, error) {
	return _ERC20SimpleSwap.Contract.HardDeposits(&_ERC20SimpleSwap.CallOpts, arg0)
}

// Issuer is a free data retrieval call binding the contract method 0x1d143848.
//
// Solidity: function issuer() view returns(address)
func (_ERC20SimpleSwap *ERC20SimpleSwapCaller) Issuer(opts *bind.CallOpts) (common.Address, error) {
	var out []interface{}
	err := _ERC20SimpleSwap.contract.Call(opts, &out, "issuer")

	if err != nil {
		return *new(common.Address), err
	}

	out0 := *abi.ConvertType(out[0], new(common.Address)).(*common.Address)

	return out0, err

}

// Issuer is a free data retrieval call binding the contract method 0x1d143848.
//
// Solidity: function issuer() view returns(address)
func (_ERC20SimpleSwap *ERC20SimpleSwapSession) Issuer() (common.Address, error) {
	return _ERC20SimpleSwap.Contract.Issuer(&_ERC20SimpleSwap.CallOpts)
}

// Issuer is a free data retrieval call binding the contract method 0x1d143848.
//
// Solidity: function issuer() view returns(address)
func (_ERC20SimpleSwap *ERC20SimpleSwapCallerSession) Issuer() (common.Address, error) {
	return _ERC20SimpleSwap.Contract.Issuer(&_ERC20SimpleSwap.CallOpts)
}

// LiquidBalance is a free data retrieval call binding the contract method 0xb7ec1a33.
//
// Solidity: function liquidBalance() view returns(uint256)
func (_ERC20SimpleSwap *ERC20SimpleSwapCaller) LiquidBalance(opts *bind.CallOpts) (*big.Int, error) {
	var out []interface{}
	err := _ERC20SimpleSwap.contract.Call(opts, &out, "liquidBalance")

	if err != nil {
		return *new(*big.Int), err
	}

	out0 := *abi.ConvertType(out[0], new(*big.Int)).(**big.Int)

	return out0, err

}

// LiquidBalance is a free data retrieval call binding the contract method 0xb7ec1a33.
//
// Solidity: function liquidBalance() view returns(uint256)
func (_ERC20SimpleSwap *ERC20SimpleSwapSession) LiquidBalance() (*big.Int, error) {
	return _ERC20SimpleSwap.Contract.LiquidBalance(&_ERC20SimpleSwap.CallOpts)
}

// LiquidBalance is a free data retrieval call binding the contract method 0xb7ec1a33.
//
// Solidity: function liquidBalance() view returns(uint256)
func (_ERC20SimpleSwap *ERC20SimpleSwapCallerSession) LiquidBalance() (*big.Int, error) {
	return _ERC20SimpleSwap.Contract.LiquidBalance(&_ERC20SimpleSwap.CallOpts)
}

// LiquidBalanceFor is a free data retrieval call binding the contract method 0xc76a4d31.
//
// Solidity: function liquidBalanceFor(address beneficiary) view returns(uint256)
func (_ERC20SimpleSwap *ERC20SimpleSwapCaller) LiquidBalanceFor(opts *bind.CallOpts, beneficiary common.Address) (*big.Int, error) {
	var out []interface{}
	err := _ERC20SimpleSwap.contract.Call(opts, &out, "liquidBalanceFor", beneficiary)

	if err != nil {
		return *new(*big.Int), err
	}

	out0 := *abi.ConvertType(out[0], new(*big.Int)).(**big.Int)

	return out0, err

}

// LiquidBalanceFor is a free data retrieval call binding the contract method 0xc76a4d31.
//
// Solidity: function liquidBalanceFor(address beneficiary) view returns(uint256)
func (_ERC20SimpleSwap *ERC20SimpleSwapSession) LiquidBalanceFor(beneficiary common.Address) (*big.Int, error) {
	return _ERC20SimpleSwap.Contract.LiquidBalanceFor(&_ERC20SimpleSwap.CallOpts, beneficiary)
}

// LiquidBalanceFor is a free data retrieval call binding the contract method 0xc76a4d31.
//
// Solidity: function liquidBalanceFor(address beneficiary) view returns(uint256)
func (_ERC20SimpleSwap *ERC20SimpleSwapCallerSession) LiquidBalanceFor(beneficiary common.Address) (*big.Int, error) {
	return _ERC20SimpleSwap.Contract.LiquidBalanceFor(&_ERC20SimpleSwap.CallOpts, beneficiary)
}

// PaidOut is a free data retrieval call binding the contract method 0x81f03fcb.
//
// Solidity: function paidOut(address ) view returns(uint256)
func (_ERC20SimpleSwap *ERC20SimpleSwapCaller) PaidOut(opts *bind.CallOpts, arg0 common.Address) (*big.Int, error) {
	var out []interface{}
	err := _ERC20SimpleSwap.contract.Call(opts, &out, "paidOut", arg0)

	if err != nil {
		return *new(*big.Int), err
	}

	out0 := *abi.ConvertType(out[0], new(*big.Int)).(**big.Int)

	return out0, err

}

// PaidOut is a free data retrieval call binding the contract method 0x81f03fcb.
//
// Solidity: function paidOut(address ) view returns(uint256)
func (_ERC20SimpleSwap *ERC20SimpleSwapSession) PaidOut(arg0 common.Address) (*big.Int, error) {
	return _ERC20SimpleSwap.Contract.PaidOut(&_ERC20SimpleSwap.CallOpts, arg0)
}

// PaidOut is a free data retrieval call binding the contract method 0x81f03fcb.
//
// Solidity: function paidOut(address ) view returns(uint256)
func (_ERC20SimpleSwap *ERC20SimpleSwapCallerSession) PaidOut(arg0 common.Address) (*big.Int, error) {
	return _ERC20SimpleSwap.Contract.PaidOut(&_ERC20SimpleSwap.CallOpts, arg0)
}

// Token is a free data retrieval call binding the contract method 0xfc0c546a.
//
// Solidity: function token() view returns(address)
func (_ERC20SimpleSwap *ERC20SimpleSwapCaller) Token(opts *bind.CallOpts) (common.Address, error) {
	var out []interface{}
	err := _ERC20SimpleSwap.contract.Call(opts, &out, "token")

	if err != nil {
		return *new(common.Address), err
	}

	out0 := *abi.ConvertType(out[0], new(common.Address)).(*common.Address)

	return out0, err

}

// Token is a free data retrieval call binding the contract method 0xfc0c546a.
//
// Solidity: function token() view returns(address)
func (_ERC20SimpleSwap *ERC20SimpleSwapSession) Token() (common.Address, error) {
	return _ERC20SimpleSwap.Contract.Token(&_ERC20SimpleSwap.CallOpts)
}

// Token is a free data retrieval call binding the contract method 0xfc0c546a.
//
// Solidity: function token() view returns(address)
func (_ERC20SimpleSwap *ERC20SimpleSwapCallerSession) Token() (common.Address, error) {
	return _ERC20SimpleSwap.Contract.Token(&_ERC20SimpleSwap.CallOpts)
}

// TotalHardDeposit is a free data retrieval call binding the contract method 0xe0bcf13a.
//
// Solidity: function totalHardDeposit() view returns(uint256)
func (_ERC20SimpleSwap *ERC20SimpleSwapCaller) TotalHardDeposit(opts *bind.CallOpts) (*big.Int, error) {
	var out []interface{}
	err := _ERC20SimpleSwap.contract.Call(opts, &out, "totalHardDeposit")

	if err != nil {
		return *new(*big.Int), err
	}

	out0 := *abi.ConvertType(out[0], new(*big.Int)).(**big.Int)

	return out0, err

}

// TotalHardDeposit is a free data retrieval call binding the contract method 0xe0bcf13a.
//
// Solidity: function totalHardDeposit() view returns(uint256)
func (_ERC20SimpleSwap *ERC20SimpleSwapSession) TotalHardDeposit() (*big.Int, error) {
	return _ERC20SimpleSwap.Contract.TotalHardDeposit(&_ERC20SimpleSwap.CallOpts)
}

// TotalHardDeposit is a free data retrieval call binding the contract method 0xe0bcf13a.
//
// Solidity: function totalHardDeposit() view returns(uint256)
func (_ERC20SimpleSwap *ERC20SimpleSwapCallerSession) TotalHardDeposit() (*big.Int, error) {
	return _ERC20SimpleSwap.Contract.TotalHardDeposit(&_ERC20SimpleSwap.CallOpts)
}

// TotalPaidOut is a free data retrieval call binding the contract method 0x1357e1dc.
//
// Solidity: function totalPaidOut() view returns(uint256)
func (_ERC20SimpleSwap *ERC20SimpleSwapCaller) TotalPaidOut(opts *bind.CallOpts) (*big.Int, error) {
	var out []interface{}
	err := _ERC20SimpleSwap.contract.Call(opts, &out, "totalPaidOut")

	if err != nil {
		return *new(*big.Int), err
	}

	out0 := *abi.ConvertType(out[0], new(*big.Int)).(**big.Int)

	return out0, err

}

// TotalPaidOut is a free data retrieval call binding the contract method 0x1357e1dc.
//
// Solidity: function totalPaidOut() view returns(uint256)
func (_ERC20SimpleSwap *ERC20SimpleSwapSession) TotalPaidOut() (*big.Int, error) {
	return _ERC20SimpleSwap.Contract.TotalPaidOut(&_ERC20SimpleSwap.CallOpts)
}

// TotalPaidOut is a free data retrieval call binding the contract method 0x1357e1dc.
//
// Solidity: function totalPaidOut() view returns(uint256)
func (_ERC20SimpleSwap *ERC20SimpleSwapCallerSession) TotalPaidOut() (*big.Int, error) {
	return _ERC20SimpleSwap.Contract.TotalPaidOut(&_ERC20SimpleSwap.CallOpts)
}

// CashCheque is a paid mutator transaction binding the contract method 0x1633fb1d.
//
// Solidity: function cashCheque(address beneficiary, address recipient, uint256 cumulativePayout, bytes beneficiarySig, uint256 callerPayout, bytes issuerSig) returns()
func (_ERC20SimpleSwap *ERC20SimpleSwapTransactor) CashCheque(opts *bind.TransactOpts, beneficiary common.Address, recipient common.Address, cumulativePayout *big.Int, beneficiarySig []byte, callerPayout *big.Int, issuerSig []byte) (*types.Transaction, error) {
	return _ERC20SimpleSwap.contract.Transact(opts, "cashCheque", beneficiary, recipient, cumulativePayout, beneficiarySig, callerPayout, issuerSig)
}

// CashCheque is a paid mutator transaction binding the contract method 0x1633fb1d.
//
// Solidity: function cashCheque(address beneficiary, address recipient, uint256 cumulativePayout, bytes beneficiarySig, uint256 callerPayout, bytes issuerSig) returns()
func (_ERC20SimpleSwap *ERC20SimpleSwapSession) CashCheque(beneficiary common.Address, recipient common.Address, cumulativePayout *big.Int, beneficiarySig []byte, callerPayout *big.Int, issuerSig []byte) (*types.Transaction, error) {
	return _ERC20SimpleSwap.Contract.CashCheque(&_ERC20SimpleSwap.TransactOpts, beneficiary, recipient, cumulativePayout, beneficiarySig, callerPayout, issuerSig)
}

// CashCheque is a paid mutator transaction binding the contract method 0x1633fb1d.
//
// Solidity: function cashCheque(address beneficiary, address recipient, uint256 cumulativePayout, bytes beneficiarySig, uint256 callerPayout, bytes issuerSig) returns()
func (_ERC20SimpleSwap *ERC20SimpleSwapTransactorSession) CashCheque(beneficiary common.Address, recipient common.Address, cumulativePayout *big.Int, beneficiarySig []byte, callerPayout *big.Int, issuerSig []byte) (*types.Transaction, error) {
	return _ERC20SimpleSwap.Contract.CashCheque(&_ERC20SimpleSwap.TransactOpts, beneficiary, recipient, cumulativePayout, beneficiarySig, callerPayout, issuerSig)
}

// CashChequeBeneficiary is a paid mutator transaction binding the contract method 0x0d5f2659.
//
// Solidity: function cashChequeBeneficiary(address recipient, uint256 cumulativePayout, bytes issuerSig) returns()
func (_ERC20SimpleSwap *ERC20SimpleSwapTransactor) CashChequeBeneficiary(opts *bind.TransactOpts, recipient common.Address, cumulativePayout *big.Int, issuerSig []byte) (*types.Transaction, error) {
	return _ERC20SimpleSwap.contract.Transact(opts, "cashChequeBeneficiary", recipient, cumulativePayout, issuerSig)
}

// CashChequeBeneficiary is a paid mutator transaction binding the contract method 0x0d5f2659.
//
// Solidity: function cashChequeBeneficiary(address recipient, uint256 cumulativePayout, bytes issuerSig) returns()
func (_ERC20SimpleSwap *ERC20SimpleSwapSession) CashChequeBeneficiary(recipient common.Address, cumulativePayout *big.Int, issuerSig []byte) (*types.Transaction, error) {
	return _ERC20SimpleSwap.Contract.CashChequeBeneficiary(&_ERC20SimpleSwap.TransactOpts, recipient, cumulativePayout, issuerSig)
}

// CashChequeBeneficiary is a paid mutator transaction binding the contract method 0x0d5f2659.
//
// Solidity: function cashChequeBeneficiary(address recipient, uint256 cumulativePayout, bytes issuerSig) returns()
func (_ERC20SimpleSwap *ERC20SimpleSwapTransactorSession) CashChequeBeneficiary(recipient common.Address, cumulativePayout *big.Int, issuerSig []byte) (*types.Transaction, error) {
	return _ERC20SimpleSwap.Contract.CashChequeBeneficiary(&_ERC20SimpleSwap.TransactOpts, recipient, cumulativePayout, issuerSig)
}

// DecreaseHardDeposit is a paid mutator transaction binding the contract method 0x946f46a2.
//
// Solidity: function decreaseHardDeposit(address beneficiary) returns()
func (_ERC20SimpleSwap *ERC20SimpleSwapTransactor) DecreaseHardDeposit(opts *bind.TransactOpts, beneficiary common.Address) (*types.Transaction, error) {
	return _ERC20SimpleSwap.contract.Transact(opts, "decreaseHardDeposit", beneficiary)
}

// DecreaseHardDeposit is a paid mutator transaction binding the contract method 0x946f46a2.
//
// Solidity: function decreaseHardDeposit(address beneficiary) returns()
func (_ERC20SimpleSwap *ERC20SimpleSwapSession) DecreaseHardDeposit(beneficiary common.Address) (*types.Transaction, error) {
	return _ERC20SimpleSwap.Contract.DecreaseHardDeposit(&_ERC20SimpleSwap.TransactOpts, beneficiary)
}

// DecreaseHardDeposit is a paid mutator transaction binding the contract method 0x946f46a2.
//
// Solidity: function decreaseHardDeposit(address beneficiary) returns()
func (_ERC20SimpleSwap *ERC20SimpleSwapTransactorSession) DecreaseHardDeposit(beneficiary common.Address) (*types.Transaction, error) {
	return _ERC20SimpleSwap.Contract.DecreaseHardDeposit(&_ERC20SimpleSwap.TransactOpts, beneficiary)
}

// IncreaseHardDeposit is a paid mutator transaction binding the contract method 0x338f3fed.
//
// Solidity: function increaseHardDeposit(address beneficiary, uint256 amount) returns()
func (_ERC20SimpleSwap *ERC20SimpleSwapTransactor) IncreaseHardDeposit(opts *bind.TransactOpts, beneficiary common.Address, amount *big.Int) (*types.Transaction, error) {
	return _ERC20SimpleSwap.contract.Transact(opts, "increaseHardDeposit", beneficiary, amount)
}

// IncreaseHardDeposit is a paid mutator transaction binding the contract method 0x338f3fed.
//
// Solidity: function increaseHardDeposit(address beneficiary, uint256 amount) returns()
func (_ERC20SimpleSwap *ERC20SimpleSwapSession) IncreaseHardDeposit(beneficiary common.Address, amount *big.Int) (*types.Transaction, error) {
	return _ERC20SimpleSwap.Contract.IncreaseHardDeposit(&_ERC20SimpleSwap.TransactOpts, beneficiary, amount)
}

// IncreaseHardDeposit is a paid mutator transaction binding the contract method 0x338f3fed.
//
// Solidity: function increaseHardDeposit(address beneficiary, uint256 amount) returns()
func (_ERC20SimpleSwap *ERC20SimpleSwapTransactorSession) IncreaseHardDeposit(beneficiary common.Address, amount *big.Int) (*types.Transaction, error) {
	return _ERC20SimpleSwap.Contract.IncreaseHardDeposit(&_ERC20SimpleSwap.TransactOpts, beneficiary, amount)
}

// PrepareDecreaseHardDeposit is a paid mutator transaction binding the contract method 0xb7770350.
//
// Solidity: function prepareDecreaseHardDeposit(address beneficiary, uint256 decreaseAmount) returns()
func (_ERC20SimpleSwap *ERC20SimpleSwapTransactor) PrepareDecreaseHardDeposit(opts *bind.TransactOpts, beneficiary common.Address, decreaseAmount *big.Int) (*types.Transaction, error) {
	return _ERC20SimpleSwap.contract.Transact(opts, "prepareDecreaseHardDeposit", beneficiary, decreaseAmount)
}

// PrepareDecreaseHardDeposit is a paid mutator transaction binding the contract method 0xb7770350.
//
// Solidity: function prepareDecreaseHardDeposit(address beneficiary, uint256 decreaseAmount) returns()
func (_ERC20SimpleSwap *ERC20SimpleSwapSession) PrepareDecreaseHardDeposit(beneficiary common.Address, decreaseAmount *big.Int) (*types.Transaction, error) {
	return _ERC20SimpleSwap.Contract.PrepareDecreaseHardDeposit(&_ERC20SimpleSwap.TransactOpts, beneficiary, decreaseAmount)
}

// PrepareDecreaseHardDeposit is a paid mutator transaction binding the contract method 0xb7770350.
//
// Solidity: function prepareDecreaseHardDeposit(address beneficiary, uint256 decreaseAmount) returns()
func (_ERC20SimpleSwap *ERC20SimpleSwapTransactorSession) PrepareDecreaseHardDeposit(beneficiary common.Address, decreaseAmount *big.Int) (*types.Transaction, error) {
	return _ERC20SimpleSwap.Contract.PrepareDecreaseHardDeposit(&_ERC20SimpleSwap.TransactOpts, beneficiary, decreaseAmount)
}

// SetCustomHardDepositTimeout is a paid mutator transaction binding the contract method 0xd4c9a8e8.
//
// Solidity: function setCustomHardDepositTimeout(address beneficiary, uint256 hardDepositTimeout, bytes beneficiarySig) returns()
func (_ERC20SimpleSwap *ERC20SimpleSwapTransactor) SetCustomHardDepositTimeout(opts *bind.TransactOpts, beneficiary common.Address, hardDepositTimeout *big.Int, beneficiarySig []byte) (*types.Transaction, error) {
	return _ERC20SimpleSwap.contract.Transact(opts, "setCustomHardDepositTimeout", beneficiary, hardDepositTimeout, beneficiarySig)
}

// SetCustomHardDepositTimeout is a paid mutator transaction binding the contract method 0xd4c9a8e8.
//
// Solidity: function setCustomHardDepositTimeout(address beneficiary, uint256 hardDepositTimeout, bytes beneficiarySig) returns()
func (_ERC20SimpleSwap *ERC20SimpleSwapSession) SetCustomHardDepositTimeout(beneficiary common.Address, hardDepositTimeout *big.Int, beneficiarySig []byte) (*types.Transaction, error) {
	return _ERC20SimpleSwap.Contract.SetCustomHardDepositTimeout(&_ERC20SimpleSwap.TransactOpts, beneficiary, hardDepositTimeout, beneficiarySig)
}

// SetCustomHardDepositTimeout is a paid mutator transaction binding the contract method 0xd4c9a8e8.
//
// Solidity: function setCustomHardDepositTimeout(address beneficiary, uint256 hardDepositTimeout, bytes beneficiarySig) returns()
func (_ERC20SimpleSwap *ERC20SimpleSwapTransactorSession) SetCustomHardDepositTimeout(beneficiary common.Address, hardDepositTimeout *big.Int, beneficiarySig []byte) (*types.Transaction, error) {
	return _ERC20SimpleSwap.Contract.SetCustomHardDepositTimeout(&_ERC20SimpleSwap.TransactOpts, beneficiary, hardDepositTimeout, beneficiarySig)
}

// Withdraw is a paid mutator transaction binding the contract method 0x2e1a7d4d.
//
// Solidity: function withdraw(uint256 amount) returns()
func (_ERC20SimpleSwap *ERC20SimpleSwapTransactor) Withdraw(opts *bind.TransactOpts, amount *big.Int) (*types.Transaction, error) {
	return _ERC20SimpleSwap.contract.Transact(opts, "withdraw", amount)
}

// Withdraw is a paid mutator transaction binding the contract method 0x2e1a7d4d.
//
// Solidity: function withdraw(uint256 amount) returns()
func (_ERC20SimpleSwap *ERC20SimpleSwapSession) Withdraw(amount *big.Int) (*types.Transaction, error) {
	return _ERC20SimpleSwap.Contract.Withdraw(&_ERC20SimpleSwap.TransactOpts, amount)
}

// Withdraw is a paid mutator transaction binding the contract method 0x2e1a7d4d.
//
// Solidity: function withdraw(uint256 amount) returns()
func (_ERC20SimpleSwap *ERC20SimpleSwapTransactorSession) Withdraw(amount *big.Int) (*types.Transaction, error) {
	return _ERC20SimpleSwap.Contract.Withdraw(&_ERC20SimpleSwap.TransactOpts, amount)
}

// ERC20SimpleSwapChequeBouncedIterator is returned from FilterChequeBounced and is used to iterate over the raw logs and unpacked data for ChequeBounced events raised by the ERC20SimpleSwap contract.
type ERC20SimpleSwapChequeBouncedIterator struct {
	Event *ERC20SimpleSwapChequeBounced // Event containing the contract specifics and raw log

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
func (it *ERC20SimpleSwapChequeBouncedIterator) Next() bool {
	// If the iterator failed, stop iterating
	if it.fail != nil {
		return false
	}
	// If the iterator completed, deliver directly whatever's available
	if it.done {
		select {
		case log := <-it.logs:
			it.Event = new(ERC20SimpleSwapChequeBounced)
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
		it.Event = new(ERC20SimpleSwapChequeBounced)
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
func (it *ERC20SimpleSwapChequeBouncedIterator) Error() error {
	return it.fail
}

// Close terminates the iteration process, releasing any pending underlying
// resources.
func (it *ERC20SimpleSwapChequeBouncedIterator) Close() error {
	it.sub.Unsubscribe()
	return nil
}

// ERC20SimpleSwapChequeBounced represents a ChequeBounced event raised by the ERC20SimpleSwap contract.
type ERC20SimpleSwapChequeBounced struct {
	Raw types.Log // Blockchain specific contextual infos
}

// FilterChequeBounced is a free log retrieval operation binding the contract event 0x3f4449c047e11092ec54dc0751b6b4817a9162745de856c893a26e611d18ffc4.
//
// Solidity: event ChequeBounced()
func (_ERC20SimpleSwap *ERC20SimpleSwapFilterer) FilterChequeBounced(opts *bind.FilterOpts) (*ERC20SimpleSwapChequeBouncedIterator, error) {

	logs, sub, err := _ERC20SimpleSwap.contract.FilterLogs(opts, "ChequeBounced")
	if err != nil {
		return nil, err
	}
	return &ERC20SimpleSwapChequeBouncedIterator{contract: _ERC20SimpleSwap.contract, event: "ChequeBounced", logs: logs, sub: sub}, nil
}

// WatchChequeBounced is a free log subscription operation binding the contract event 0x3f4449c047e11092ec54dc0751b6b4817a9162745de856c893a26e611d18ffc4.
//
// Solidity: event ChequeBounced()
func (_ERC20SimpleSwap *ERC20SimpleSwapFilterer) WatchChequeBounced(opts *bind.WatchOpts, sink chan<- *ERC20SimpleSwapChequeBounced) (event.Subscription, error) {

	logs, sub, err := _ERC20SimpleSwap.contract.WatchLogs(opts, "ChequeBounced")
	if err != nil {
		return nil, err
	}
	return event.NewSubscription(func(quit <-chan struct{}) error {
		defer sub.Unsubscribe()
		for {
			select {
			case log := <-logs:
				// New log arrived, parse the event and forward to the user
				event := new(ERC20SimpleSwapChequeBounced)
				if err := _ERC20SimpleSwap.contract.UnpackLog(event, "ChequeBounced", log); err != nil {
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

// ParseChequeBounced is a log parse operation binding the contract event 0x3f4449c047e11092ec54dc0751b6b4817a9162745de856c893a26e611d18ffc4.
//
// Solidity: event ChequeBounced()
func (_ERC20SimpleSwap *ERC20SimpleSwapFilterer) ParseChequeBounced(log types.Log) (*ERC20SimpleSwapChequeBounced, error) {
	event := new(ERC20SimpleSwapChequeBounced)
	if err := _ERC20SimpleSwap.contract.UnpackLog(event, "ChequeBounced", log); err != nil {
		return nil, err
	}
	event.Raw = log
	return event, nil
}

// ERC20SimpleSwapChequeCashedIterator is returned from FilterChequeCashed and is used to iterate over the raw logs and unpacked data for ChequeCashed events raised by the ERC20SimpleSwap contract.
type ERC20SimpleSwapChequeCashedIterator struct {
	Event *ERC20SimpleSwapChequeCashed // Event containing the contract specifics and raw log

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
func (it *ERC20SimpleSwapChequeCashedIterator) Next() bool {
	// If the iterator failed, stop iterating
	if it.fail != nil {
		return false
	}
	// If the iterator completed, deliver directly whatever's available
	if it.done {
		select {
		case log := <-it.logs:
			it.Event = new(ERC20SimpleSwapChequeCashed)
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
		it.Event = new(ERC20SimpleSwapChequeCashed)
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
func (it *ERC20SimpleSwapChequeCashedIterator) Error() error {
	return it.fail
}

// Close terminates the iteration process, releasing any pending underlying
// resources.
func (it *ERC20SimpleSwapChequeCashedIterator) Close() error {
	it.sub.Unsubscribe()
	return nil
}

// ERC20SimpleSwapChequeCashed represents a ChequeCashed event raised by the ERC20SimpleSwap contract.
type ERC20SimpleSwapChequeCashed struct {
	Beneficiary      common.Address
	Recipient        common.Address
	Caller           common.Address
	TotalPayout      *big.Int
	CumulativePayout *big.Int
	CallerPayout     *big.Int
	Raw              types.Log // Blockchain specific contextual infos
}

// FilterChequeCashed is a free log retrieval operation binding the contract event 0x950494fc3642fae5221b6c32e0e45765c95ebb382a04a71b160db0843e74c99f.
//
// Solidity: event ChequeCashed(address indexed beneficiary, address indexed recipient, address indexed caller, uint256 totalPayout, uint256 cumulativePayout, uint256 callerPayout)
func (_ERC20SimpleSwap *ERC20SimpleSwapFilterer) FilterChequeCashed(opts *bind.FilterOpts, beneficiary []common.Address, recipient []common.Address, caller []common.Address) (*ERC20SimpleSwapChequeCashedIterator, error) {

	var beneficiaryRule []interface{}
	for _, beneficiaryItem := range beneficiary {
		beneficiaryRule = append(beneficiaryRule, beneficiaryItem)
	}
	var recipientRule []interface{}
	for _, recipientItem := range recipient {
		recipientRule = append(recipientRule, recipientItem)
	}
	var callerRule []interface{}
	for _, callerItem := range caller {
		callerRule = append(callerRule, callerItem)
	}

	logs, sub, err := _ERC20SimpleSwap.contract.FilterLogs(opts, "ChequeCashed", beneficiaryRule, recipientRule, callerRule)
	if err != nil {
		return nil, err
	}
	return &ERC20SimpleSwapChequeCashedIterator{contract: _ERC20SimpleSwap.contract, event: "ChequeCashed", logs: logs, sub: sub}, nil
}

// WatchChequeCashed is a free log subscription operation binding the contract event 0x950494fc3642fae5221b6c32e0e45765c95ebb382a04a71b160db0843e74c99f.
//
// Solidity: event ChequeCashed(address indexed beneficiary, address indexed recipient, address indexed caller, uint256 totalPayout, uint256 cumulativePayout, uint256 callerPayout)
func (_ERC20SimpleSwap *ERC20SimpleSwapFilterer) WatchChequeCashed(opts *bind.WatchOpts, sink chan<- *ERC20SimpleSwapChequeCashed, beneficiary []common.Address, recipient []common.Address, caller []common.Address) (event.Subscription, error) {

	var beneficiaryRule []interface{}
	for _, beneficiaryItem := range beneficiary {
		beneficiaryRule = append(beneficiaryRule, beneficiaryItem)
	}
	var recipientRule []interface{}
	for _, recipientItem := range recipient {
		recipientRule = append(recipientRule, recipientItem)
	}
	var callerRule []interface{}
	for _, callerItem := range caller {
		callerRule = append(callerRule, callerItem)
	}

	logs, sub, err := _ERC20SimpleSwap.contract.WatchLogs(opts, "ChequeCashed", beneficiaryRule, recipientRule, callerRule)
	if err != nil {
		return nil, err
	}
	return event.NewSubscription(func(quit <-chan struct{}) error {
		defer sub.Unsubscribe()
		for {
			select {
			case log := <-logs:
				// New log arrived, parse the event and forward to the user
				event := new(ERC20SimpleSwapChequeCashed)
				if err := _ERC20SimpleSwap.contract.UnpackLog(event, "ChequeCashed", log); err != nil {
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

// ParseChequeCashed is a log parse operation binding the contract event 0x950494fc3642fae5221b6c32e0e45765c95ebb382a04a71b160db0843e74c99f.
//
// Solidity: event ChequeCashed(address indexed beneficiary, address indexed recipient, address indexed caller, uint256 totalPayout, uint256 cumulativePayout, uint256 callerPayout)
func (_ERC20SimpleSwap *ERC20SimpleSwapFilterer) ParseChequeCashed(log types.Log) (*ERC20SimpleSwapChequeCashed, error) {
	event := new(ERC20SimpleSwapChequeCashed)
	if err := _ERC20SimpleSwap.contract.UnpackLog(event, "ChequeCashed", log); err != nil {
		return nil, err
	}
	event.Raw = log
	return event, nil
}

// ERC20SimpleSwapHardDepositAmountChangedIterator is returned from FilterHardDepositAmountChanged and is used to iterate over the raw logs and unpacked data for HardDepositAmountChanged events raised by the ERC20SimpleSwap contract.
type ERC20SimpleSwapHardDepositAmountChangedIterator struct {
	Event *ERC20SimpleSwapHardDepositAmountChanged // Event containing the contract specifics and raw log

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
func (it *ERC20SimpleSwapHardDepositAmountChangedIterator) Next() bool {
	// If the iterator failed, stop iterating
	if it.fail != nil {
		return false
	}
	// If the iterator completed, deliver directly whatever's available
	if it.done {
		select {
		case log := <-it.logs:
			it.Event = new(ERC20SimpleSwapHardDepositAmountChanged)
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
		it.Event = new(ERC20SimpleSwapHardDepositAmountChanged)
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
func (it *ERC20SimpleSwapHardDepositAmountChangedIterator) Error() error {
	return it.fail
}

// Close terminates the iteration process, releasing any pending underlying
// resources.
func (it *ERC20SimpleSwapHardDepositAmountChangedIterator) Close() error {
	it.sub.Unsubscribe()
	return nil
}

// ERC20SimpleSwapHardDepositAmountChanged represents a HardDepositAmountChanged event raised by the ERC20SimpleSwap contract.
type ERC20SimpleSwapHardDepositAmountChanged struct {
	Beneficiary common.Address
	Amount      *big.Int
	Raw         types.Log // Blockchain specific contextual infos
}

// FilterHardDepositAmountChanged is a free log retrieval operation binding the contract event 0x2506c43272ded05d095b91dbba876e66e46888157d3e078db5691496e96c5fad.
//
// Solidity: event HardDepositAmountChanged(address indexed beneficiary, uint256 amount)
func (_ERC20SimpleSwap *ERC20SimpleSwapFilterer) FilterHardDepositAmountChanged(opts *bind.FilterOpts, beneficiary []common.Address) (*ERC20SimpleSwapHardDepositAmountChangedIterator, error) {

	var beneficiaryRule []interface{}
	for _, beneficiaryItem := range beneficiary {
		beneficiaryRule = append(beneficiaryRule, beneficiaryItem)
	}

	logs, sub, err := _ERC20SimpleSwap.contract.FilterLogs(opts, "HardDepositAmountChanged", beneficiaryRule)
	if err != nil {
		return nil, err
	}
	return &ERC20SimpleSwapHardDepositAmountChangedIterator{contract: _ERC20SimpleSwap.contract, event: "HardDepositAmountChanged", logs: logs, sub: sub}, nil
}

// WatchHardDepositAmountChanged is a free log subscription operation binding the contract event 0x2506c43272ded05d095b91dbba876e66e46888157d3e078db5691496e96c5fad.
//
// Solidity: event HardDepositAmountChanged(address indexed beneficiary, uint256 amount)
func (_ERC20SimpleSwap *ERC20SimpleSwapFilterer) WatchHardDepositAmountChanged(opts *bind.WatchOpts, sink chan<- *ERC20SimpleSwapHardDepositAmountChanged, beneficiary []common.Address) (event.Subscription, error) {

	var beneficiaryRule []interface{}
	for _, beneficiaryItem := range beneficiary {
		beneficiaryRule = append(beneficiaryRule, beneficiaryItem)
	}

	logs, sub, err := _ERC20SimpleSwap.contract.WatchLogs(opts, "HardDepositAmountChanged", beneficiaryRule)
	if err != nil {
		return nil, err
	}
	return event.NewSubscription(func(quit <-chan struct{}) error {
		defer sub.Unsubscribe()
		for {
			select {
			case log := <-logs:
				// New log arrived, parse the event and forward to the user
				event := new(ERC20SimpleSwapHardDepositAmountChanged)
				if err := _ERC20SimpleSwap.contract.UnpackLog(event, "HardDepositAmountChanged", log); err != nil {
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

// ParseHardDepositAmountChanged is a log parse operation binding the contract event 0x2506c43272ded05d095b91dbba876e66e46888157d3e078db5691496e96c5fad.
//
// Solidity: event HardDepositAmountChanged(address indexed beneficiary, uint256 amount)
func (_ERC20SimpleSwap *ERC20SimpleSwapFilterer) ParseHardDepositAmountChanged(log types.Log) (*ERC20SimpleSwapHardDepositAmountChanged, error) {
	event := new(ERC20SimpleSwapHardDepositAmountChanged)
	if err := _ERC20SimpleSwap.contract.UnpackLog(event, "HardDepositAmountChanged", log); err != nil {
		return nil, err
	}
	event.Raw = log
	return event, nil
}

// ERC20SimpleSwapHardDepositDecreasePreparedIterator is returned from FilterHardDepositDecreasePrepared and is used to iterate over the raw logs and unpacked data for HardDepositDecreasePrepared events raised by the ERC20SimpleSwap contract.
type ERC20SimpleSwapHardDepositDecreasePreparedIterator struct {
	Event *ERC20SimpleSwapHardDepositDecreasePrepared // Event containing the contract specifics and raw log

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
func (it *ERC20SimpleSwapHardDepositDecreasePreparedIterator) Next() bool {
	// If the iterator failed, stop iterating
	if it.fail != nil {
		return false
	}
	// If the iterator completed, deliver directly whatever's available
	if it.done {
		select {
		case log := <-it.logs:
			it.Event = new(ERC20SimpleSwapHardDepositDecreasePrepared)
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
		it.Event = new(ERC20SimpleSwapHardDepositDecreasePrepared)
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
func (it *ERC20SimpleSwapHardDepositDecreasePreparedIterator) Error() error {
	return it.fail
}

// Close terminates the iteration process, releasing any pending underlying
// resources.
func (it *ERC20SimpleSwapHardDepositDecreasePreparedIterator) Close() error {
	it.sub.Unsubscribe()
	return nil
}

// ERC20SimpleSwapHardDepositDecreasePrepared represents a HardDepositDecreasePrepared event raised by the ERC20SimpleSwap contract.
type ERC20SimpleSwapHardDepositDecreasePrepared struct {
	Beneficiary    common.Address
	DecreaseAmount *big.Int
	Raw            types.Log // Blockchain specific contextual infos
}

// FilterHardDepositDecreasePrepared is a free log retrieval operation binding the contract event 0xc8305077b495025ec4c1d977b176a762c350bb18cad4666ce1ee85c32b78698a.
//
// Solidity: event HardDepositDecreasePrepared(address indexed beneficiary, uint256 decreaseAmount)
func (_ERC20SimpleSwap *ERC20SimpleSwapFilterer) FilterHardDepositDecreasePrepared(opts *bind.FilterOpts, beneficiary []common.Address) (*ERC20SimpleSwapHardDepositDecreasePreparedIterator, error) {

	var beneficiaryRule []interface{}
	for _, beneficiaryItem := range beneficiary {
		beneficiaryRule = append(beneficiaryRule, beneficiaryItem)
	}

	logs, sub, err := _ERC20SimpleSwap.contract.FilterLogs(opts, "HardDepositDecreasePrepared", beneficiaryRule)
	if err != nil {
		return nil, err
	}
	return &ERC20SimpleSwapHardDepositDecreasePreparedIterator{contract: _ERC20SimpleSwap.contract, event: "HardDepositDecreasePrepared", logs: logs, sub: sub}, nil
}

// WatchHardDepositDecreasePrepared is a free log subscription operation binding the contract event 0xc8305077b495025ec4c1d977b176a762c350bb18cad4666ce1ee85c32b78698a.
//
// Solidity: event HardDepositDecreasePrepared(address indexed beneficiary, uint256 decreaseAmount)
func (_ERC20SimpleSwap *ERC20SimpleSwapFilterer) WatchHardDepositDecreasePrepared(opts *bind.WatchOpts, sink chan<- *ERC20SimpleSwapHardDepositDecreasePrepared, beneficiary []common.Address) (event.Subscription, error) {

	var beneficiaryRule []interface{}
	for _, beneficiaryItem := range beneficiary {
		beneficiaryRule = append(beneficiaryRule, beneficiaryItem)
	}

	logs, sub, err := _ERC20SimpleSwap.contract.WatchLogs(opts, "HardDepositDecreasePrepared", beneficiaryRule)
	if err != nil {
		return nil, err
	}
	return event.NewSubscription(func(quit <-chan struct{}) error {
		defer sub.Unsubscribe()
		for {
			select {
			case log := <-logs:
				// New log arrived, parse the event and forward to the user
				event := new(ERC20SimpleSwapHardDepositDecreasePrepared)
				if err := _ERC20SimpleSwap.contract.UnpackLog(event, "HardDepositDecreasePrepared", log); err != nil {
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

// ParseHardDepositDecreasePrepared is a log parse operation binding the contract event 0xc8305077b495025ec4c1d977b176a762c350bb18cad4666ce1ee85c32b78698a.
//
// Solidity: event HardDepositDecreasePrepared(address indexed beneficiary, uint256 decreaseAmount)
func (_ERC20SimpleSwap *ERC20SimpleSwapFilterer) ParseHardDepositDecreasePrepared(log types.Log) (*ERC20SimpleSwapHardDepositDecreasePrepared, error) {
	event := new(ERC20SimpleSwapHardDepositDecreasePrepared)
	if err := _ERC20SimpleSwap.contract.UnpackLog(event, "HardDepositDecreasePrepared", log); err != nil {
		return nil, err
	}
	event.Raw = log
	return event, nil
}

// ERC20SimpleSwapHardDepositTimeoutChangedIterator is returned from FilterHardDepositTimeoutChanged and is used to iterate over the raw logs and unpacked data for HardDepositTimeoutChanged events raised by the ERC20SimpleSwap contract.
type ERC20SimpleSwapHardDepositTimeoutChangedIterator struct {
	Event *ERC20SimpleSwapHardDepositTimeoutChanged // Event containing the contract specifics and raw log

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
func (it *ERC20SimpleSwapHardDepositTimeoutChangedIterator) Next() bool {
	// If the iterator failed, stop iterating
	if it.fail != nil {
		return false
	}
	// If the iterator completed, deliver directly whatever's available
	if it.done {
		select {
		case log := <-it.logs:
			it.Event = new(ERC20SimpleSwapHardDepositTimeoutChanged)
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
		it.Event = new(ERC20SimpleSwapHardDepositTimeoutChanged)
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
func (it *ERC20SimpleSwapHardDepositTimeoutChangedIterator) Error() error {
	return it.fail
}

// Close terminates the iteration process, releasing any pending underlying
// resources.
func (it *ERC20SimpleSwapHardDepositTimeoutChangedIterator) Close() error {
	it.sub.Unsubscribe()
	return nil
}

// ERC20SimpleSwapHardDepositTimeoutChanged represents a HardDepositTimeoutChanged event raised by the ERC20SimpleSwap contract.
type ERC20SimpleSwapHardDepositTimeoutChanged struct {
	Beneficiary common.Address
	Timeout     *big.Int
	Raw         types.Log // Blockchain specific contextual infos
}

// FilterHardDepositTimeoutChanged is a free log retrieval operation binding the contract event 0x7b816003a769eb718bd9c66bdbd2dd5827da3f92bc6645276876bd7957b08cf0.
//
// Solidity: event HardDepositTimeoutChanged(address indexed beneficiary, uint256 timeout)
func (_ERC20SimpleSwap *ERC20SimpleSwapFilterer) FilterHardDepositTimeoutChanged(opts *bind.FilterOpts, beneficiary []common.Address) (*ERC20SimpleSwapHardDepositTimeoutChangedIterator, error) {

	var beneficiaryRule []interface{}
	for _, beneficiaryItem := range beneficiary {
		beneficiaryRule = append(beneficiaryRule, beneficiaryItem)
	}

	logs, sub, err := _ERC20SimpleSwap.contract.FilterLogs(opts, "HardDepositTimeoutChanged", beneficiaryRule)
	if err != nil {
		return nil, err
	}
	return &ERC20SimpleSwapHardDepositTimeoutChangedIterator{contract: _ERC20SimpleSwap.contract, event: "HardDepositTimeoutChanged", logs: logs, sub: sub}, nil
}

// WatchHardDepositTimeoutChanged is a free log subscription operation binding the contract event 0x7b816003a769eb718bd9c66bdbd2dd5827da3f92bc6645276876bd7957b08cf0.
//
// Solidity: event HardDepositTimeoutChanged(address indexed beneficiary, uint256 timeout)
func (_ERC20SimpleSwap *ERC20SimpleSwapFilterer) WatchHardDepositTimeoutChanged(opts *bind.WatchOpts, sink chan<- *ERC20SimpleSwapHardDepositTimeoutChanged, beneficiary []common.Address) (event.Subscription, error) {

	var beneficiaryRule []interface{}
	for _, beneficiaryItem := range beneficiary {
		beneficiaryRule = append(beneficiaryRule, beneficiaryItem)
	}

	logs, sub, err := _ERC20SimpleSwap.contract.WatchLogs(opts, "HardDepositTimeoutChanged", beneficiaryRule)
	if err != nil {
		return nil, err
	}
	return event.NewSubscription(func(quit <-chan struct{}) error {
		defer sub.Unsubscribe()
		for {
			select {
			case log := <-logs:
				// New log arrived, parse the event and forward to the user
				event := new(ERC20SimpleSwapHardDepositTimeoutChanged)
				if err := _ERC20SimpleSwap.contract.UnpackLog(event, "HardDepositTimeoutChanged", log); err != nil {
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

// ParseHardDepositTimeoutChanged is a log parse operation binding the contract event 0x7b816003a769eb718bd9c66bdbd2dd5827da3f92bc6645276876bd7957b08cf0.
//
// Solidity: event HardDepositTimeoutChanged(address indexed beneficiary, uint256 timeout)
func (_ERC20SimpleSwap *ERC20SimpleSwapFilterer) ParseHardDepositTimeoutChanged(log types.Log) (*ERC20SimpleSwapHardDepositTimeoutChanged, error) {
	event := new(ERC20SimpleSwapHardDepositTimeoutChanged)
	if err := _ERC20SimpleSwap.contract.UnpackLog(event, "HardDepositTimeoutChanged", log); err != nil {
		return nil, err
	}
	event.Raw = log
	return event, nil
}

// ERC20SimpleSwapWithdrawIterator is returned from FilterWithdraw and is used to iterate over the raw logs and unpacked data for Withdraw events raised by the ERC20SimpleSwap contract.
type ERC20SimpleSwapWithdrawIterator struct {
	Event *ERC20SimpleSwapWithdraw // Event containing the contract specifics and raw log

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
func (it *ERC20SimpleSwapWithdrawIterator) Next() bool {
	// If the iterator failed, stop iterating
	if it.fail != nil {
		return false
	}
	// If the iterator completed, deliver directly whatever's available
	if it.done {
		select {
		case log := <-it.logs:
			it.Event = new(ERC20SimpleSwapWithdraw)
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
		it.Event = new(ERC20SimpleSwapWithdraw)
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
func (it *ERC20SimpleSwapWithdrawIterator) Error() error {
	return it.fail
}

// Close terminates the iteration process, releasing any pending underlying
// resources.
func (it *ERC20SimpleSwapWithdrawIterator) Close() error {
	it.sub.Unsubscribe()
	return nil
}

// ERC20SimpleSwapWithdraw represents a Withdraw event raised by the ERC20SimpleSwap contract.
type ERC20SimpleSwapWithdraw struct {
	Amount *big.Int
	Raw    types.Log // Blockchain specific contextual infos
}

// FilterWithdraw is a free log retrieval operation binding the contract event 0x5b6b431d4476a211bb7d41c20d1aab9ae2321deee0d20be3d9fc9b1093fa6e3d.
//
// Solidity: event Withdraw(uint256 amount)
func (_ERC20SimpleSwap *ERC20SimpleSwapFilterer) FilterWithdraw(opts *bind.FilterOpts) (*ERC20SimpleSwapWithdrawIterator, error) {

	logs, sub, err := _ERC20SimpleSwap.contract.FilterLogs(opts, "Withdraw")
	if err != nil {
		return nil, err
	}
	return &ERC20SimpleSwapWithdrawIterator{contract: _ERC20SimpleSwap.contract, event: "Withdraw", logs: logs, sub: sub}, nil
}

// WatchWithdraw is a free log subscription operation binding the contract event 0x5b6b431d4476a211bb7d41c20d1aab9ae2321deee0d20be3d9fc9b1093fa6e3d.
//
// Solidity: event Withdraw(uint256 amount)
func (_ERC20SimpleSwap *ERC20SimpleSwapFilterer) WatchWithdraw(opts *bind.WatchOpts, sink chan<- *ERC20SimpleSwapWithdraw) (event.Subscription, error) {

	logs, sub, err := _ERC20SimpleSwap.contract.WatchLogs(opts, "Withdraw")
	if err != nil {
		return nil, err
	}
	return event.NewSubscription(func(quit <-chan struct{}) error {
		defer sub.Unsubscribe()
		for {
			select {
			case log := <-logs:
				// New log arrived, parse the event and forward to the user
				event := new(ERC20SimpleSwapWithdraw)
				if err := _ERC20SimpleSwap.contract.UnpackLog(event, "Withdraw", log); err != nil {
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

// ParseWithdraw is a log parse operation binding the contract event 0x5b6b431d4476a211bb7d41c20d1aab9ae2321deee0d20be3d9fc9b1093fa6e3d.
//
// Solidity: event Withdraw(uint256 amount)
func (_ERC20SimpleSwap *ERC20SimpleSwapFilterer) ParseWithdraw(log types.Log) (*ERC20SimpleSwapWithdraw, error) {
	event := new(ERC20SimpleSwapWithdraw)
	if err := _ERC20SimpleSwap.contract.UnpackLog(event, "Withdraw", log); err != nil {
		return nil, err
	}
	event.Raw = log
	return event, nil
}

// IERC20ABI is the input ABI used to generate the binding from.
const IERC20ABI = "[{\"anonymous\":false,\"inputs\":[{\"indexed\":true,\"internalType\":\"address\",\"name\":\"owner\",\"type\":\"address\"},{\"indexed\":true,\"internalType\":\"address\",\"name\":\"spender\",\"type\":\"address\"},{\"indexed\":false,\"internalType\":\"uint256\",\"name\":\"value\",\"type\":\"uint256\"}],\"name\":\"Approval\",\"type\":\"event\"},{\"anonymous\":false,\"inputs\":[{\"indexed\":true,\"internalType\":\"address\",\"name\":\"from\",\"type\":\"address\"},{\"indexed\":true,\"internalType\":\"address\",\"name\":\"to\",\"type\":\"address\"},{\"indexed\":false,\"internalType\":\"uint256\",\"name\":\"value\",\"type\":\"uint256\"}],\"name\":\"Transfer\",\"type\":\"event\"},{\"inputs\":[{\"internalType\":\"address\",\"name\":\"owner\",\"type\":\"address\"},{\"internalType\":\"address\",\"name\":\"spender\",\"type\":\"address\"}],\"name\":\"allowance\",\"outputs\":[{\"internalType\":\"uint256\",\"name\":\"\",\"type\":\"uint256\"}],\"stateMutability\":\"view\",\"type\":\"function\"},{\"inputs\":[{\"internalType\":\"address\",\"name\":\"spender\",\"type\":\"address\"},{\"internalType\":\"uint256\",\"name\":\"amount\",\"type\":\"uint256\"}],\"name\":\"approve\",\"outputs\":[{\"internalType\":\"bool\",\"name\":\"\",\"type\":\"bool\"}],\"stateMutability\":\"nonpayable\",\"type\":\"function\"},{\"inputs\":[{\"internalType\":\"address\",\"name\":\"account\",\"type\":\"address\"}],\"name\":\"balanceOf\",\"outputs\":[{\"internalType\":\"uint256\",\"name\":\"\",\"type\":\"uint256\"}],\"stateMutability\":\"view\",\"type\":\"function\"},{\"inputs\":[],\"name\":\"totalSupply\",\"outputs\":[{\"internalType\":\"uint256\",\"name\":\"\",\"type\":\"uint256\"}],\"stateMutability\":\"view\",\"type\":\"function\"},{\"inputs\":[{\"internalType\":\"address\",\"name\":\"recipient\",\"type\":\"address\"},{\"internalType\":\"uint256\",\"name\":\"amount\",\"type\":\"uint256\"}],\"name\":\"transfer\",\"outputs\":[{\"internalType\":\"bool\",\"name\":\"\",\"type\":\"bool\"}],\"stateMutability\":\"nonpayable\",\"type\":\"function\"},{\"inputs\":[{\"internalType\":\"address\",\"name\":\"sender\",\"type\":\"address\"},{\"internalType\":\"address\",\"name\":\"recipient\",\"type\":\"address\"},{\"internalType\":\"uint256\",\"name\":\"amount\",\"type\":\"uint256\"}],\"name\":\"transferFrom\",\"outputs\":[{\"internalType\":\"bool\",\"name\":\"\",\"type\":\"bool\"}],\"stateMutability\":\"nonpayable\",\"type\":\"function\"}]"

// IERC20 is an auto generated Go binding around an Ethereum contract.
type IERC20 struct {
	IERC20Caller     // Read-only binding to the contract
	IERC20Transactor // Write-only binding to the contract
	IERC20Filterer   // Log filterer for contract events
}

// IERC20Caller is an auto generated read-only Go binding around an Ethereum contract.
type IERC20Caller struct {
	contract *bind.BoundContract // Generic contract wrapper for the low level calls
}

// IERC20Transactor is an auto generated write-only Go binding around an Ethereum contract.
type IERC20Transactor struct {
	contract *bind.BoundContract // Generic contract wrapper for the low level calls
}

// IERC20Filterer is an auto generated log filtering Go binding around an Ethereum contract events.
type IERC20Filterer struct {
	contract *bind.BoundContract // Generic contract wrapper for the low level calls
}

// IERC20Session is an auto generated Go binding around an Ethereum contract,
// with pre-set call and transact options.
type IERC20Session struct {
	Contract     *IERC20           // Generic contract binding to set the session for
	CallOpts     bind.CallOpts     // Call options to use throughout this session
	TransactOpts bind.TransactOpts // Transaction auth options to use throughout this session
}

// IERC20CallerSession is an auto generated read-only Go binding around an Ethereum contract,
// with pre-set call options.
type IERC20CallerSession struct {
	Contract *IERC20Caller // Generic contract caller binding to set the session for
	CallOpts bind.CallOpts // Call options to use throughout this session
}

// IERC20TransactorSession is an auto generated write-only Go binding around an Ethereum contract,
// with pre-set transact options.
type IERC20TransactorSession struct {
	Contract     *IERC20Transactor // Generic contract transactor binding to set the session for
	TransactOpts bind.TransactOpts // Transaction auth options to use throughout this session
}

// IERC20Raw is an auto generated low-level Go binding around an Ethereum contract.
type IERC20Raw struct {
	Contract *IERC20 // Generic contract binding to access the raw methods on
}

// IERC20CallerRaw is an auto generated low-level read-only Go binding around an Ethereum contract.
type IERC20CallerRaw struct {
	Contract *IERC20Caller // Generic read-only contract binding to access the raw methods on
}

// IERC20TransactorRaw is an auto generated low-level write-only Go binding around an Ethereum contract.
type IERC20TransactorRaw struct {
	Contract *IERC20Transactor // Generic write-only contract binding to access the raw methods on
}

// NewIERC20 creates a new instance of IERC20, bound to a specific deployed contract.
func NewIERC20(address common.Address, backend bind.ContractBackend) (*IERC20, error) {
	contract, err := bindIERC20(address, backend, backend, backend)
	if err != nil {
		return nil, err
	}
	return &IERC20{IERC20Caller: IERC20Caller{contract: contract}, IERC20Transactor: IERC20Transactor{contract: contract}, IERC20Filterer: IERC20Filterer{contract: contract}}, nil
}

// NewIERC20Caller creates a new read-only instance of IERC20, bound to a specific deployed contract.
func NewIERC20Caller(address common.Address, caller bind.ContractCaller) (*IERC20Caller, error) {
	contract, err := bindIERC20(address, caller, nil, nil)
	if err != nil {
		return nil, err
	}
	return &IERC20Caller{contract: contract}, nil
}

// NewIERC20Transactor creates a new write-only instance of IERC20, bound to a specific deployed contract.
func NewIERC20Transactor(address common.Address, transactor bind.ContractTransactor) (*IERC20Transactor, error) {
	contract, err := bindIERC20(address, nil, transactor, nil)
	if err != nil {
		return nil, err
	}
	return &IERC20Transactor{contract: contract}, nil
}

// NewIERC20Filterer creates a new log filterer instance of IERC20, bound to a specific deployed contract.
func NewIERC20Filterer(address common.Address, filterer bind.ContractFilterer) (*IERC20Filterer, error) {
	contract, err := bindIERC20(address, nil, nil, filterer)
	if err != nil {
		return nil, err
	}
	return &IERC20Filterer{contract: contract}, nil
}

// bindIERC20 binds a generic wrapper to an already deployed contract.
func bindIERC20(address common.Address, caller bind.ContractCaller, transactor bind.ContractTransactor, filterer bind.ContractFilterer) (*bind.BoundContract, error) {
	parsed, err := abi.JSON(strings.NewReader(IERC20ABI))
	if err != nil {
		return nil, err
	}
	return bind.NewBoundContract(address, parsed, caller, transactor, filterer), nil
}

// Call invokes the (constant) contract method with params as input values and
// sets the output to result. The result type might be a single field for simple
// returns, a slice of interfaces for anonymous returns and a struct for named
// returns.
func (_IERC20 *IERC20Raw) Call(opts *bind.CallOpts, result *[]interface{}, method string, params ...interface{}) error {
	return _IERC20.Contract.IERC20Caller.contract.Call(opts, result, method, params...)
}

// Transfer initiates a plain transaction to move funds to the contract, calling
// its default method if one is available.
func (_IERC20 *IERC20Raw) Transfer(opts *bind.TransactOpts) (*types.Transaction, error) {
	return _IERC20.Contract.IERC20Transactor.contract.Transfer(opts)
}

// Transact invokes the (paid) contract method with params as input values.
func (_IERC20 *IERC20Raw) Transact(opts *bind.TransactOpts, method string, params ...interface{}) (*types.Transaction, error) {
	return _IERC20.Contract.IERC20Transactor.contract.Transact(opts, method, params...)
}

// Call invokes the (constant) contract method with params as input values and
// sets the output to result. The result type might be a single field for simple
// returns, a slice of interfaces for anonymous returns and a struct for named
// returns.
func (_IERC20 *IERC20CallerRaw) Call(opts *bind.CallOpts, result *[]interface{}, method string, params ...interface{}) error {
	return _IERC20.Contract.contract.Call(opts, result, method, params...)
}

// Transfer initiates a plain transaction to move funds to the contract, calling
// its default method if one is available.
func (_IERC20 *IERC20TransactorRaw) Transfer(opts *bind.TransactOpts) (*types.Transaction, error) {
	return _IERC20.Contract.contract.Transfer(opts)
}

// Transact invokes the (paid) contract method with params as input values.
func (_IERC20 *IERC20TransactorRaw) Transact(opts *bind.TransactOpts, method string, params ...interface{}) (*types.Transaction, error) {
	return _IERC20.Contract.contract.Transact(opts, method, params...)
}

// Allowance is a free data retrieval call binding the contract method 0xdd62ed3e.
//
// Solidity: function allowance(address owner, address spender) view returns(uint256)
func (_IERC20 *IERC20Caller) Allowance(opts *bind.CallOpts, owner common.Address, spender common.Address) (*big.Int, error) {
	var out []interface{}
	err := _IERC20.contract.Call(opts, &out, "allowance", owner, spender)

	if err != nil {
		return *new(*big.Int), err
	}

	out0 := *abi.ConvertType(out[0], new(*big.Int)).(**big.Int)

	return out0, err

}

// Allowance is a free data retrieval call binding the contract method 0xdd62ed3e.
//
// Solidity: function allowance(address owner, address spender) view returns(uint256)
func (_IERC20 *IERC20Session) Allowance(owner common.Address, spender common.Address) (*big.Int, error) {
	return _IERC20.Contract.Allowance(&_IERC20.CallOpts, owner, spender)
}

// Allowance is a free data retrieval call binding the contract method 0xdd62ed3e.
//
// Solidity: function allowance(address owner, address spender) view returns(uint256)
func (_IERC20 *IERC20CallerSession) Allowance(owner common.Address, spender common.Address) (*big.Int, error) {
	return _IERC20.Contract.Allowance(&_IERC20.CallOpts, owner, spender)
}

// BalanceOf is a free data retrieval call binding the contract method 0x70a08231.
//
// Solidity: function balanceOf(address account) view returns(uint256)
func (_IERC20 *IERC20Caller) BalanceOf(opts *bind.CallOpts, account common.Address) (*big.Int, error) {
	var out []interface{}
	err := _IERC20.contract.Call(opts, &out, "balanceOf", account)

	if err != nil {
		return *new(*big.Int), err
	}

	out0 := *abi.ConvertType(out[0], new(*big.Int)).(**big.Int)

	return out0, err

}

// BalanceOf is a free data retrieval call binding the contract method 0x70a08231.
//
// Solidity: function balanceOf(address account) view returns(uint256)
func (_IERC20 *IERC20Session) BalanceOf(account common.Address) (*big.Int, error) {
	return _IERC20.Contract.BalanceOf(&_IERC20.CallOpts, account)
}

// BalanceOf is a free data retrieval call binding the contract method 0x70a08231.
//
// Solidity: function balanceOf(address account) view returns(uint256)
func (_IERC20 *IERC20CallerSession) BalanceOf(account common.Address) (*big.Int, error) {
	return _IERC20.Contract.BalanceOf(&_IERC20.CallOpts, account)
}

// TotalSupply is a free data retrieval call binding the contract method 0x18160ddd.
//
// Solidity: function totalSupply() view returns(uint256)
func (_IERC20 *IERC20Caller) TotalSupply(opts *bind.CallOpts) (*big.Int, error) {
	var out []interface{}
	err := _IERC20.contract.Call(opts, &out, "totalSupply")

	if err != nil {
		return *new(*big.Int), err
	}

	out0 := *abi.ConvertType(out[0], new(*big.Int)).(**big.Int)

	return out0, err

}

// TotalSupply is a free data retrieval call binding the contract method 0x18160ddd.
//
// Solidity: function totalSupply() view returns(uint256)
func (_IERC20 *IERC20Session) TotalSupply() (*big.Int, error) {
	return _IERC20.Contract.TotalSupply(&_IERC20.CallOpts)
}

// TotalSupply is a free data retrieval call binding the contract method 0x18160ddd.
//
// Solidity: function totalSupply() view returns(uint256)
func (_IERC20 *IERC20CallerSession) TotalSupply() (*big.Int, error) {
	return _IERC20.Contract.TotalSupply(&_IERC20.CallOpts)
}

// Approve is a paid mutator transaction binding the contract method 0x095ea7b3.
//
// Solidity: function approve(address spender, uint256 amount) returns(bool)
func (_IERC20 *IERC20Transactor) Approve(opts *bind.TransactOpts, spender common.Address, amount *big.Int) (*types.Transaction, error) {
	return _IERC20.contract.Transact(opts, "approve", spender, amount)
}

// Approve is a paid mutator transaction binding the contract method 0x095ea7b3.
//
// Solidity: function approve(address spender, uint256 amount) returns(bool)
func (_IERC20 *IERC20Session) Approve(spender common.Address, amount *big.Int) (*types.Transaction, error) {
	return _IERC20.Contract.Approve(&_IERC20.TransactOpts, spender, amount)
}

// Approve is a paid mutator transaction binding the contract method 0x095ea7b3.
//
// Solidity: function approve(address spender, uint256 amount) returns(bool)
func (_IERC20 *IERC20TransactorSession) Approve(spender common.Address, amount *big.Int) (*types.Transaction, error) {
	return _IERC20.Contract.Approve(&_IERC20.TransactOpts, spender, amount)
}

// Transfer is a paid mutator transaction binding the contract method 0xa9059cbb.
//
// Solidity: function transfer(address recipient, uint256 amount) returns(bool)
func (_IERC20 *IERC20Transactor) Transfer(opts *bind.TransactOpts, recipient common.Address, amount *big.Int) (*types.Transaction, error) {
	return _IERC20.contract.Transact(opts, "transfer", recipient, amount)
}

// Transfer is a paid mutator transaction binding the contract method 0xa9059cbb.
//
// Solidity: function transfer(address recipient, uint256 amount) returns(bool)
func (_IERC20 *IERC20Session) Transfer(recipient common.Address, amount *big.Int) (*types.Transaction, error) {
	return _IERC20.Contract.Transfer(&_IERC20.TransactOpts, recipient, amount)
}

// Transfer is a paid mutator transaction binding the contract method 0xa9059cbb.
//
// Solidity: function transfer(address recipient, uint256 amount) returns(bool)
func (_IERC20 *IERC20TransactorSession) Transfer(recipient common.Address, amount *big.Int) (*types.Transaction, error) {
	return _IERC20.Contract.Transfer(&_IERC20.TransactOpts, recipient, amount)
}

// TransferFrom is a paid mutator transaction binding the contract method 0x23b872dd.
//
// Solidity: function transferFrom(address sender, address recipient, uint256 amount) returns(bool)
func (_IERC20 *IERC20Transactor) TransferFrom(opts *bind.TransactOpts, sender common.Address, recipient common.Address, amount *big.Int) (*types.Transaction, error) {
	return _IERC20.contract.Transact(opts, "transferFrom", sender, recipient, amount)
}

// TransferFrom is a paid mutator transaction binding the contract method 0x23b872dd.
//
// Solidity: function transferFrom(address sender, address recipient, uint256 amount) returns(bool)
func (_IERC20 *IERC20Session) TransferFrom(sender common.Address, recipient common.Address, amount *big.Int) (*types.Transaction, error) {
	return _IERC20.Contract.TransferFrom(&_IERC20.TransactOpts, sender, recipient, amount)
}

// TransferFrom is a paid mutator transaction binding the contract method 0x23b872dd.
//
// Solidity: function transferFrom(address sender, address recipient, uint256 amount) returns(bool)
func (_IERC20 *IERC20TransactorSession) TransferFrom(sender common.Address, recipient common.Address, amount *big.Int) (*types.Transaction, error) {
	return _IERC20.Contract.TransferFrom(&_IERC20.TransactOpts, sender, recipient, amount)
}

// IERC20ApprovalIterator is returned from FilterApproval and is used to iterate over the raw logs and unpacked data for Approval events raised by the IERC20 contract.
type IERC20ApprovalIterator struct {
	Event *IERC20Approval // Event containing the contract specifics and raw log

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
func (it *IERC20ApprovalIterator) Next() bool {
	// If the iterator failed, stop iterating
	if it.fail != nil {
		return false
	}
	// If the iterator completed, deliver directly whatever's available
	if it.done {
		select {
		case log := <-it.logs:
			it.Event = new(IERC20Approval)
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
		it.Event = new(IERC20Approval)
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
func (it *IERC20ApprovalIterator) Error() error {
	return it.fail
}

// Close terminates the iteration process, releasing any pending underlying
// resources.
func (it *IERC20ApprovalIterator) Close() error {
	it.sub.Unsubscribe()
	return nil
}

// IERC20Approval represents a Approval event raised by the IERC20 contract.
type IERC20Approval struct {
	Owner   common.Address
	Spender common.Address
	Value   *big.Int
	Raw     types.Log // Blockchain specific contextual infos
}

// FilterApproval is a free log retrieval operation binding the contract event 0x8c5be1e5ebec7d5bd14f71427d1e84f3dd0314c0f7b2291e5b200ac8c7c3b925.
//
// Solidity: event Approval(address indexed owner, address indexed spender, uint256 value)
func (_IERC20 *IERC20Filterer) FilterApproval(opts *bind.FilterOpts, owner []common.Address, spender []common.Address) (*IERC20ApprovalIterator, error) {

	var ownerRule []interface{}
	for _, ownerItem := range owner {
		ownerRule = append(ownerRule, ownerItem)
	}
	var spenderRule []interface{}
	for _, spenderItem := range spender {
		spenderRule = append(spenderRule, spenderItem)
	}

	logs, sub, err := _IERC20.contract.FilterLogs(opts, "Approval", ownerRule, spenderRule)
	if err != nil {
		return nil, err
	}
	return &IERC20ApprovalIterator{contract: _IERC20.contract, event: "Approval", logs: logs, sub: sub}, nil
}

// WatchApproval is a free log subscription operation binding the contract event 0x8c5be1e5ebec7d5bd14f71427d1e84f3dd0314c0f7b2291e5b200ac8c7c3b925.
//
// Solidity: event Approval(address indexed owner, address indexed spender, uint256 value)
func (_IERC20 *IERC20Filterer) WatchApproval(opts *bind.WatchOpts, sink chan<- *IERC20Approval, owner []common.Address, spender []common.Address) (event.Subscription, error) {

	var ownerRule []interface{}
	for _, ownerItem := range owner {
		ownerRule = append(ownerRule, ownerItem)
	}
	var spenderRule []interface{}
	for _, spenderItem := range spender {
		spenderRule = append(spenderRule, spenderItem)
	}

	logs, sub, err := _IERC20.contract.WatchLogs(opts, "Approval", ownerRule, spenderRule)
	if err != nil {
		return nil, err
	}
	return event.NewSubscription(func(quit <-chan struct{}) error {
		defer sub.Unsubscribe()
		for {
			select {
			case log := <-logs:
				// New log arrived, parse the event and forward to the user
				event := new(IERC20Approval)
				if err := _IERC20.contract.UnpackLog(event, "Approval", log); err != nil {
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
func (_IERC20 *IERC20Filterer) ParseApproval(log types.Log) (*IERC20Approval, error) {
	event := new(IERC20Approval)
	if err := _IERC20.contract.UnpackLog(event, "Approval", log); err != nil {
		return nil, err
	}
	event.Raw = log
	return event, nil
}

// IERC20TransferIterator is returned from FilterTransfer and is used to iterate over the raw logs and unpacked data for Transfer events raised by the IERC20 contract.
type IERC20TransferIterator struct {
	Event *IERC20Transfer // Event containing the contract specifics and raw log

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
func (it *IERC20TransferIterator) Next() bool {
	// If the iterator failed, stop iterating
	if it.fail != nil {
		return false
	}
	// If the iterator completed, deliver directly whatever's available
	if it.done {
		select {
		case log := <-it.logs:
			it.Event = new(IERC20Transfer)
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
		it.Event = new(IERC20Transfer)
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
func (it *IERC20TransferIterator) Error() error {
	return it.fail
}

// Close terminates the iteration process, releasing any pending underlying
// resources.
func (it *IERC20TransferIterator) Close() error {
	it.sub.Unsubscribe()
	return nil
}

// IERC20Transfer represents a Transfer event raised by the IERC20 contract.
type IERC20Transfer struct {
	From  common.Address
	To    common.Address
	Value *big.Int
	Raw   types.Log // Blockchain specific contextual infos
}

// FilterTransfer is a free log retrieval operation binding the contract event 0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef.
//
// Solidity: event Transfer(address indexed from, address indexed to, uint256 value)
func (_IERC20 *IERC20Filterer) FilterTransfer(opts *bind.FilterOpts, from []common.Address, to []common.Address) (*IERC20TransferIterator, error) {

	var fromRule []interface{}
	for _, fromItem := range from {
		fromRule = append(fromRule, fromItem)
	}
	var toRule []interface{}
	for _, toItem := range to {
		toRule = append(toRule, toItem)
	}

	logs, sub, err := _IERC20.contract.FilterLogs(opts, "Transfer", fromRule, toRule)
	if err != nil {
		return nil, err
	}
	return &IERC20TransferIterator{contract: _IERC20.contract, event: "Transfer", logs: logs, sub: sub}, nil
}

// WatchTransfer is a free log subscription operation binding the contract event 0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef.
//
// Solidity: event Transfer(address indexed from, address indexed to, uint256 value)
func (_IERC20 *IERC20Filterer) WatchTransfer(opts *bind.WatchOpts, sink chan<- *IERC20Transfer, from []common.Address, to []common.Address) (event.Subscription, error) {

	var fromRule []interface{}
	for _, fromItem := range from {
		fromRule = append(fromRule, fromItem)
	}
	var toRule []interface{}
	for _, toItem := range to {
		toRule = append(toRule, toItem)
	}

	logs, sub, err := _IERC20.contract.WatchLogs(opts, "Transfer", fromRule, toRule)
	if err != nil {
		return nil, err
	}
	return event.NewSubscription(func(quit <-chan struct{}) error {
		defer sub.Unsubscribe()
		for {
			select {
			case log := <-logs:
				// New log arrived, parse the event and forward to the user
				event := new(IERC20Transfer)
				if err := _IERC20.contract.UnpackLog(event, "Transfer", log); err != nil {
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
func (_IERC20 *IERC20Filterer) ParseTransfer(log types.Log) (*IERC20Transfer, error) {
	event := new(IERC20Transfer)
	if err := _IERC20.contract.UnpackLog(event, "Transfer", log); err != nil {
		return nil, err
	}
	event.Raw = log
	return event, nil
}

// MathABI is the input ABI used to generate the binding from.
const MathABI = "[]"

// MathBin is the compiled bytecode used for deploying new contracts.
var MathBin = "0x60566023600b82828239805160001a607314601657fe5b30600052607381538281f3fe73000000000000000000000000000000000000000030146080604052600080fdfea26469706673582212206d62f7e57091ceca3ef3f612add4f71888a59bf8702a1e0508b15ce5853e532664736f6c634300060c0033"

// DeployMath deploys a new Ethereum contract, binding an instance of Math to it.
func DeployMath(auth *bind.TransactOpts, backend bind.ContractBackend) (common.Address, *types.Transaction, *Math, error) {
	parsed, err := abi.JSON(strings.NewReader(MathABI))
	if err != nil {
		return common.Address{}, nil, nil, err
	}

	address, tx, contract, err := bind.DeployContract(auth, parsed, common.FromHex(MathBin), backend)
	if err != nil {
		return common.Address{}, nil, nil, err
	}
	return address, tx, &Math{MathCaller: MathCaller{contract: contract}, MathTransactor: MathTransactor{contract: contract}, MathFilterer: MathFilterer{contract: contract}}, nil
}

// Math is an auto generated Go binding around an Ethereum contract.
type Math struct {
	MathCaller     // Read-only binding to the contract
	MathTransactor // Write-only binding to the contract
	MathFilterer   // Log filterer for contract events
}

// MathCaller is an auto generated read-only Go binding around an Ethereum contract.
type MathCaller struct {
	contract *bind.BoundContract // Generic contract wrapper for the low level calls
}

// MathTransactor is an auto generated write-only Go binding around an Ethereum contract.
type MathTransactor struct {
	contract *bind.BoundContract // Generic contract wrapper for the low level calls
}

// MathFilterer is an auto generated log filtering Go binding around an Ethereum contract events.
type MathFilterer struct {
	contract *bind.BoundContract // Generic contract wrapper for the low level calls
}

// MathSession is an auto generated Go binding around an Ethereum contract,
// with pre-set call and transact options.
type MathSession struct {
	Contract     *Math             // Generic contract binding to set the session for
	CallOpts     bind.CallOpts     // Call options to use throughout this session
	TransactOpts bind.TransactOpts // Transaction auth options to use throughout this session
}

// MathCallerSession is an auto generated read-only Go binding around an Ethereum contract,
// with pre-set call options.
type MathCallerSession struct {
	Contract *MathCaller   // Generic contract caller binding to set the session for
	CallOpts bind.CallOpts // Call options to use throughout this session
}

// MathTransactorSession is an auto generated write-only Go binding around an Ethereum contract,
// with pre-set transact options.
type MathTransactorSession struct {
	Contract     *MathTransactor   // Generic contract transactor binding to set the session for
	TransactOpts bind.TransactOpts // Transaction auth options to use throughout this session
}

// MathRaw is an auto generated low-level Go binding around an Ethereum contract.
type MathRaw struct {
	Contract *Math // Generic contract binding to access the raw methods on
}

// MathCallerRaw is an auto generated low-level read-only Go binding around an Ethereum contract.
type MathCallerRaw struct {
	Contract *MathCaller // Generic read-only contract binding to access the raw methods on
}

// MathTransactorRaw is an auto generated low-level write-only Go binding around an Ethereum contract.
type MathTransactorRaw struct {
	Contract *MathTransactor // Generic write-only contract binding to access the raw methods on
}

// NewMath creates a new instance of Math, bound to a specific deployed contract.
func NewMath(address common.Address, backend bind.ContractBackend) (*Math, error) {
	contract, err := bindMath(address, backend, backend, backend)
	if err != nil {
		return nil, err
	}
	return &Math{MathCaller: MathCaller{contract: contract}, MathTransactor: MathTransactor{contract: contract}, MathFilterer: MathFilterer{contract: contract}}, nil
}

// NewMathCaller creates a new read-only instance of Math, bound to a specific deployed contract.
func NewMathCaller(address common.Address, caller bind.ContractCaller) (*MathCaller, error) {
	contract, err := bindMath(address, caller, nil, nil)
	if err != nil {
		return nil, err
	}
	return &MathCaller{contract: contract}, nil
}

// NewMathTransactor creates a new write-only instance of Math, bound to a specific deployed contract.
func NewMathTransactor(address common.Address, transactor bind.ContractTransactor) (*MathTransactor, error) {
	contract, err := bindMath(address, nil, transactor, nil)
	if err != nil {
		return nil, err
	}
	return &MathTransactor{contract: contract}, nil
}

// NewMathFilterer creates a new log filterer instance of Math, bound to a specific deployed contract.
func NewMathFilterer(address common.Address, filterer bind.ContractFilterer) (*MathFilterer, error) {
	contract, err := bindMath(address, nil, nil, filterer)
	if err != nil {
		return nil, err
	}
	return &MathFilterer{contract: contract}, nil
}

// bindMath binds a generic wrapper to an already deployed contract.
func bindMath(address common.Address, caller bind.ContractCaller, transactor bind.ContractTransactor, filterer bind.ContractFilterer) (*bind.BoundContract, error) {
	parsed, err := abi.JSON(strings.NewReader(MathABI))
	if err != nil {
		return nil, err
	}
	return bind.NewBoundContract(address, parsed, caller, transactor, filterer), nil
}

// Call invokes the (constant) contract method with params as input values and
// sets the output to result. The result type might be a single field for simple
// returns, a slice of interfaces for anonymous returns and a struct for named
// returns.
func (_Math *MathRaw) Call(opts *bind.CallOpts, result *[]interface{}, method string, params ...interface{}) error {
	return _Math.Contract.MathCaller.contract.Call(opts, result, method, params...)
}

// Transfer initiates a plain transaction to move funds to the contract, calling
// its default method if one is available.
func (_Math *MathRaw) Transfer(opts *bind.TransactOpts) (*types.Transaction, error) {
	return _Math.Contract.MathTransactor.contract.Transfer(opts)
}

// Transact invokes the (paid) contract method with params as input values.
func (_Math *MathRaw) Transact(opts *bind.TransactOpts, method string, params ...interface{}) (*types.Transaction, error) {
	return _Math.Contract.MathTransactor.contract.Transact(opts, method, params...)
}

// Call invokes the (constant) contract method with params as input values and
// sets the output to result. The result type might be a single field for simple
// returns, a slice of interfaces for anonymous returns and a struct for named
// returns.
func (_Math *MathCallerRaw) Call(opts *bind.CallOpts, result *[]interface{}, method string, params ...interface{}) error {
	return _Math.Contract.contract.Call(opts, result, method, params...)
}

// Transfer initiates a plain transaction to move funds to the contract, calling
// its default method if one is available.
func (_Math *MathTransactorRaw) Transfer(opts *bind.TransactOpts) (*types.Transaction, error) {
	return _Math.Contract.contract.Transfer(opts)
}

// Transact invokes the (paid) contract method with params as input values.
func (_Math *MathTransactorRaw) Transact(opts *bind.TransactOpts, method string, params ...interface{}) (*types.Transaction, error) {
	return _Math.Contract.contract.Transact(opts, method, params...)
}

// SafeMathABI is the input ABI used to generate the binding from.
const SafeMathABI = "[]"

// SafeMathBin is the compiled bytecode used for deploying new contracts.
var SafeMathBin = "0x60566023600b82828239805160001a607314601657fe5b30600052607381538281f3fe73000000000000000000000000000000000000000030146080604052600080fdfea2646970667358221220d06274b61ed691aad311060842db0b6608956929c97a7d60bbd3289f9f6e8e5164736f6c634300060c0033"

// DeploySafeMath deploys a new Ethereum contract, binding an instance of SafeMath to it.
func DeploySafeMath(auth *bind.TransactOpts, backend bind.ContractBackend) (common.Address, *types.Transaction, *SafeMath, error) {
	parsed, err := abi.JSON(strings.NewReader(SafeMathABI))
	if err != nil {
		return common.Address{}, nil, nil, err
	}

	address, tx, contract, err := bind.DeployContract(auth, parsed, common.FromHex(SafeMathBin), backend)
	if err != nil {
		return common.Address{}, nil, nil, err
	}
	return address, tx, &SafeMath{SafeMathCaller: SafeMathCaller{contract: contract}, SafeMathTransactor: SafeMathTransactor{contract: contract}, SafeMathFilterer: SafeMathFilterer{contract: contract}}, nil
}

// SafeMath is an auto generated Go binding around an Ethereum contract.
type SafeMath struct {
	SafeMathCaller     // Read-only binding to the contract
	SafeMathTransactor // Write-only binding to the contract
	SafeMathFilterer   // Log filterer for contract events
}

// SafeMathCaller is an auto generated read-only Go binding around an Ethereum contract.
type SafeMathCaller struct {
	contract *bind.BoundContract // Generic contract wrapper for the low level calls
}

// SafeMathTransactor is an auto generated write-only Go binding around an Ethereum contract.
type SafeMathTransactor struct {
	contract *bind.BoundContract // Generic contract wrapper for the low level calls
}

// SafeMathFilterer is an auto generated log filtering Go binding around an Ethereum contract events.
type SafeMathFilterer struct {
	contract *bind.BoundContract // Generic contract wrapper for the low level calls
}

// SafeMathSession is an auto generated Go binding around an Ethereum contract,
// with pre-set call and transact options.
type SafeMathSession struct {
	Contract     *SafeMath         // Generic contract binding to set the session for
	CallOpts     bind.CallOpts     // Call options to use throughout this session
	TransactOpts bind.TransactOpts // Transaction auth options to use throughout this session
}

// SafeMathCallerSession is an auto generated read-only Go binding around an Ethereum contract,
// with pre-set call options.
type SafeMathCallerSession struct {
	Contract *SafeMathCaller // Generic contract caller binding to set the session for
	CallOpts bind.CallOpts   // Call options to use throughout this session
}

// SafeMathTransactorSession is an auto generated write-only Go binding around an Ethereum contract,
// with pre-set transact options.
type SafeMathTransactorSession struct {
	Contract     *SafeMathTransactor // Generic contract transactor binding to set the session for
	TransactOpts bind.TransactOpts   // Transaction auth options to use throughout this session
}

// SafeMathRaw is an auto generated low-level Go binding around an Ethereum contract.
type SafeMathRaw struct {
	Contract *SafeMath // Generic contract binding to access the raw methods on
}

// SafeMathCallerRaw is an auto generated low-level read-only Go binding around an Ethereum contract.
type SafeMathCallerRaw struct {
	Contract *SafeMathCaller // Generic read-only contract binding to access the raw methods on
}

// SafeMathTransactorRaw is an auto generated low-level write-only Go binding around an Ethereum contract.
type SafeMathTransactorRaw struct {
	Contract *SafeMathTransactor // Generic write-only contract binding to access the raw methods on
}

// NewSafeMath creates a new instance of SafeMath, bound to a specific deployed contract.
func NewSafeMath(address common.Address, backend bind.ContractBackend) (*SafeMath, error) {
	contract, err := bindSafeMath(address, backend, backend, backend)
	if err != nil {
		return nil, err
	}
	return &SafeMath{SafeMathCaller: SafeMathCaller{contract: contract}, SafeMathTransactor: SafeMathTransactor{contract: contract}, SafeMathFilterer: SafeMathFilterer{contract: contract}}, nil
}

// NewSafeMathCaller creates a new read-only instance of SafeMath, bound to a specific deployed contract.
func NewSafeMathCaller(address common.Address, caller bind.ContractCaller) (*SafeMathCaller, error) {
	contract, err := bindSafeMath(address, caller, nil, nil)
	if err != nil {
		return nil, err
	}
	return &SafeMathCaller{contract: contract}, nil
}

// NewSafeMathTransactor creates a new write-only instance of SafeMath, bound to a specific deployed contract.
func NewSafeMathTransactor(address common.Address, transactor bind.ContractTransactor) (*SafeMathTransactor, error) {
	contract, err := bindSafeMath(address, nil, transactor, nil)
	if err != nil {
		return nil, err
	}
	return &SafeMathTransactor{contract: contract}, nil
}

// NewSafeMathFilterer creates a new log filterer instance of SafeMath, bound to a specific deployed contract.
func NewSafeMathFilterer(address common.Address, filterer bind.ContractFilterer) (*SafeMathFilterer, error) {
	contract, err := bindSafeMath(address, nil, nil, filterer)
	if err != nil {
		return nil, err
	}
	return &SafeMathFilterer{contract: contract}, nil
}

// bindSafeMath binds a generic wrapper to an already deployed contract.
func bindSafeMath(address common.Address, caller bind.ContractCaller, transactor bind.ContractTransactor, filterer bind.ContractFilterer) (*bind.BoundContract, error) {
	parsed, err := abi.JSON(strings.NewReader(SafeMathABI))
	if err != nil {
		return nil, err
	}
	return bind.NewBoundContract(address, parsed, caller, transactor, filterer), nil
}

// Call invokes the (constant) contract method with params as input values and
// sets the output to result. The result type might be a single field for simple
// returns, a slice of interfaces for anonymous returns and a struct for named
// returns.
func (_SafeMath *SafeMathRaw) Call(opts *bind.CallOpts, result *[]interface{}, method string, params ...interface{}) error {
	return _SafeMath.Contract.SafeMathCaller.contract.Call(opts, result, method, params...)
}

// Transfer initiates a plain transaction to move funds to the contract, calling
// its default method if one is available.
func (_SafeMath *SafeMathRaw) Transfer(opts *bind.TransactOpts) (*types.Transaction, error) {
	return _SafeMath.Contract.SafeMathTransactor.contract.Transfer(opts)
}

// Transact invokes the (paid) contract method with params as input values.
func (_SafeMath *SafeMathRaw) Transact(opts *bind.TransactOpts, method string, params ...interface{}) (*types.Transaction, error) {
	return _SafeMath.Contract.SafeMathTransactor.contract.Transact(opts, method, params...)
}

// Call invokes the (constant) contract method with params as input values and
// sets the output to result. The result type might be a single field for simple
// returns, a slice of interfaces for anonymous returns and a struct for named
// returns.
func (_SafeMath *SafeMathCallerRaw) Call(opts *bind.CallOpts, result *[]interface{}, method string, params ...interface{}) error {
	return _SafeMath.Contract.contract.Call(opts, result, method, params...)
}

// Transfer initiates a plain transaction to move funds to the contract, calling
// its default method if one is available.
func (_SafeMath *SafeMathTransactorRaw) Transfer(opts *bind.TransactOpts) (*types.Transaction, error) {
	return _SafeMath.Contract.contract.Transfer(opts)
}

// Transact invokes the (paid) contract method with params as input values.
func (_SafeMath *SafeMathTransactorRaw) Transact(opts *bind.TransactOpts, method string, params ...interface{}) (*types.Transaction, error) {
	return _SafeMath.Contract.contract.Transact(opts, method, params...)
}

// SimpleSwapFactoryABI is the input ABI used to generate the binding from.
const SimpleSwapFactoryABI = "[{\"inputs\":[{\"internalType\":\"address\",\"name\":\"_ERC20Address\",\"type\":\"address\"}],\"stateMutability\":\"nonpayable\",\"type\":\"constructor\"},{\"anonymous\":false,\"inputs\":[{\"indexed\":false,\"internalType\":\"address\",\"name\":\"contractAddress\",\"type\":\"address\"}],\"name\":\"SimpleSwapDeployed\",\"type\":\"event\"},{\"inputs\":[],\"name\":\"ERC20Address\",\"outputs\":[{\"internalType\":\"address\",\"name\":\"\",\"type\":\"address\"}],\"stateMutability\":\"view\",\"type\":\"function\"},{\"inputs\":[{\"internalType\":\"address\",\"name\":\"issuer\",\"type\":\"address\"},{\"internalType\":\"uint256\",\"name\":\"defaultHardDepositTimeoutDuration\",\"type\":\"uint256\"}],\"name\":\"deploySimpleSwap\",\"outputs\":[{\"internalType\":\"address\",\"name\":\"\",\"type\":\"address\"}],\"stateMutability\":\"nonpayable\",\"type\":\"function\"},{\"inputs\":[{\"internalType\":\"address\",\"name\":\"\",\"type\":\"address\"}],\"name\":\"deployedContracts\",\"outputs\":[{\"internalType\":\"bool\",\"name\":\"\",\"type\":\"bool\"}],\"stateMutability\":\"view\",\"type\":\"function\"}]"

// SimpleSwapFactoryBin is the compiled bytecode used for deploying new contracts.
var SimpleSwapFactoryBin = "0x608060405234801561001057600080fd5b50604051611c1b380380611c1b8339818101604052602081101561003357600080fd5b5051600180546001600160a01b0319166001600160a01b03909216919091179055611bb8806100636000396000f3fe608060405234801561001057600080fd5b50600436106100415760003560e01c8063576d727114610046578063a6021ace1461008e578063c70242ad14610096575b600080fd5b6100726004803603604081101561005c57600080fd5b506001600160a01b0381351690602001356100d0565b604080516001600160a01b039092168252519081900360200190f35b610072610198565b6100bc600480360360208110156100ac57600080fd5b50356001600160a01b03166101a7565b604080519115158252519081900360200190f35b600154604051600091829185916001600160a01b03169085906100f2906101bc565b80846001600160a01b03168152602001836001600160a01b031681526020018281526020019350505050604051809103906000f080158015610138573d6000803e3d6000fd5b506001600160a01b03811660008181526020818152604091829020805460ff19166001179055815192835290519293507fc0ffc525a1c7689549d7f79b49eca900e61ac49b43d977f680bcc3b36224c00492918290030190a19392505050565b6001546001600160a01b031681565b60006020819052908152604090205460ff1681565b6119b9806101ca8339019056fe608060405234801561001057600080fd5b506040516119b93803806119b98339818101604052606081101561003357600080fd5b5080516020820151604090920151600680546001600160a01b039384166001600160a01b0319918216179091556001805493909416921691909117909155600055611936806100836000396000f3fe608060405234801561001057600080fd5b50600436106101425760003560e01c8063b6343b0d116100b8578063b7ec1a331161007c578063b7ec1a33146104e2578063c49f91d3146104ea578063c76a4d31146104f2578063d4c9a8e814610518578063e0bcf13a146105d1578063fc0c546a146105d957610142565b8063b6343b0d1461043e578063b648b4171461048a578063b69ef8a8146104a6578063b7770350146104ae578063b7998907146104da57610142565b80631d1438481161010a5780631d1438481461037d5780632e1a7d4d146103a1578063338f3fed146103be578063488b017c146103ea57806381f03fcb146103f2578063946f46a21461041857610142565b80630d5f26591461014757806312101021146102025780631357e1dc1461021c57806315c3343f146102245780631633fb1d1461022c575b600080fd5b6102006004803603606081101561015d57600080fd5b6001600160a01b0382351691602081013591810190606081016040820135600160201b81111561018c57600080fd5b82018360208201111561019e57600080fd5b803590602001918460018302840111600160201b831117156101bf57600080fd5b91908080601f0160208091040260200160405190810160405280939291908181526020018383808284376000920191909152509295506105e1945050505050565b005b61020a6105f4565b60408051918252519081900360200190f35b61020a6105fa565b61020a610600565b610200600480360360c081101561024257600080fd5b6001600160a01b03823581169260208101359091169160408201359190810190608081016060820135600160201b81111561027c57600080fd5b82018360208201111561028e57600080fd5b803590602001918460018302840111600160201b831117156102af57600080fd5b91908080601f01602080910402602001604051908101604052809392919081815260200183838082843760009201919091525092958435959094909350604081019250602001359050600160201b81111561030957600080fd5b82018360208201111561031b57600080fd5b803590602001918460018302840111600160201b8311171561033c57600080fd5b91908080601f016020809104026020016040519081016040528093929190818152602001838380828437600092019190915250929550610624945050505050565b61038561069e565b604080516001600160a01b039092168252519081900360200190f35b610200600480360360208110156103b757600080fd5b50356106ad565b610200600480360360408110156103d457600080fd5b506001600160a01b03813516906020013561080e565b61020a61093a565b61020a6004803603602081101561040857600080fd5b50356001600160a01b031661095e565b6102006004803603602081101561042e57600080fd5b50356001600160a01b0316610970565b6104646004803603602081101561045457600080fd5b50356001600160a01b0316610a4b565b604080519485526020850193909352838301919091526060830152519081900360800190f35b610492610a72565b604080519115158252519081900360200190f35b61020a610a82565b610200600480360360408110156104c457600080fd5b506001600160a01b038135169060200135610afe565b61020a610c20565b61020a610c44565b61020a610c5f565b61020a6004803603602081101561050857600080fd5b50356001600160a01b0316610c83565b6102006004803603606081101561052e57600080fd5b6001600160a01b0382351691602081013591810190606081016040820135600160201b81111561055d57600080fd5b82018360208201111561056f57600080fd5b803590602001918460018302840111600160201b8311171561059057600080fd5b91908080601f016020809104026020016040519081016040528093929190818152602001838380828437600092019190915250929550610cb4945050505050565b61020a610dc7565b610385610dcd565b6105ef338484600085610ddc565b505050565b60005481565b60035481565b7f48ebe6deff4a5ee645c01506a026031e2a945d6f41f1f4e5098ad65347492c1281565b61063a61063430338789876111cb565b84611243565b6001600160a01b0316866001600160a01b0316146106895760405162461bcd60e51b81526004018080602001828103825260298152602001806118656029913960400191505060405180910390fd5b6106968686868585610ddc565b505050505050565b6006546001600160a01b031681565b6006546001600160a01b03163314610705576040805162461bcd60e51b815260206004820152601660248201527529b4b6b83632a9bbb0b81d103737ba1034b9b9bab2b960511b604482015290519081900360640190fd5b61070d610c44565b81111561074b5760405162461bcd60e51b81526004018080602001828103825260288152602001806118d96028913960400191505060405180910390fd5b6001546006546040805163a9059cbb60e01b81526001600160a01b039283166004820152602481018590529051919092169163a9059cbb9160448083019260209291908290030181600087803b1580156107a457600080fd5b505af11580156107b8573d6000803e3d6000fd5b505050506040513d60208110156107ce57600080fd5b505161080b5760405162461bcd60e51b815260040180806020018281038252602781526020018061183e6027913960400191505060405180910390fd5b50565b6006546001600160a01b03163314610866576040805162461bcd60e51b815260206004820152601660248201527529b4b6b83632a9bbb0b81d103737ba1034b9b9bab2b960511b604482015290519081900360640190fd5b61086e610a82565b60055461087b90836112a5565b11156108b85760405162461bcd60e51b81526004018080602001828103825260348152602001806117a16034913960400191505060405180910390fd5b6001600160a01b038216600090815260046020526040902080546108dc90836112a5565b81556005546108eb90836112a5565b60055560006003820155805460408051918252516001600160a01b038516917f2506c43272ded05d095b91dbba876e66e46888157d3e078db5691496e96c5fad919081900360200190a2505050565b7f7d824962dd0f01520922ea1766c987b1db570cd5db90bdba5ccf5e320607950281565b60026020526000908152604090205481565b6001600160a01b03811660009081526004602052604090206003810154421080159061099f5750600381015415155b6109da5760405162461bcd60e51b81526004018080602001828103825260258152602001806118196025913960400191505060405180910390fd5b600181015481546109ea91611306565b8155600060038201556001810154600554610a0491611306565b600555805460408051918252516001600160a01b038416917f2506c43272ded05d095b91dbba876e66e46888157d3e078db5691496e96c5fad919081900360200190a25050565b60046020526000908152604090208054600182015460028301546003909301549192909184565b600654600160a01b900460ff1681565b600154604080516370a0823160e01b815230600482015290516000926001600160a01b0316916370a08231916024808301926020929190829003018186803b158015610acd57600080fd5b505afa158015610ae1573d6000803e3d6000fd5b505050506040513d6020811015610af757600080fd5b5051905090565b6006546001600160a01b03163314610b56576040805162461bcd60e51b815260206004820152601660248201527529b4b6b83632a9bbb0b81d103737ba1034b9b9bab2b960511b604482015290519081900360640190fd5b6001600160a01b03821660009081526004602052604090208054821115610bae5760405162461bcd60e51b815260040180806020018281038252602781526020018061188e6027913960400191505060405180910390fd5b60008160020154600014610bc6578160020154610bca565b6000545b4281016003840155600183018490556040805185815290519192506001600160a01b038616917fc8305077b495025ec4c1d977b176a762c350bb18cad4666ce1ee85c32b78698a9181900360200190a250505050565b7fe95f353750f192082df064ca5142d3a2d6f0bef0f3ffad66d80d8af86b7a749a81565b6000610c5a600554610c54610a82565b90611306565b905090565b7fc2f8787176b8ac6bf7215b4adcc1e069bf4ab82d9ab1df05a57a91d425935b6e81565b6001600160a01b038116600090815260046020526040812054610cae90610ca8610c44565b906112a5565b92915050565b6006546001600160a01b03163314610d0c576040805162461bcd60e51b815260206004820152601660248201527529b4b6b83632a9bbb0b81d103737ba1034b9b9bab2b960511b604482015290519081900360640190fd5b610d20610d1a308585611348565b82611243565b6001600160a01b0316836001600160a01b031614610d6f5760405162461bcd60e51b81526004018080602001828103825260298152602001806118656029913960400191505060405180910390fd5b6001600160a01b038316600081815260046020908152604091829020600201859055815185815291517f7b816003a769eb718bd9c66bdbd2dd5827da3f92bc6645276876bd7957b08cf09281900390910190a2505050565b60055481565b6001546001600160a01b031681565b6006546001600160a01b03163314610e4857610dfc610d1a3087866113b1565b6006546001600160a01b03908116911614610e485760405162461bcd60e51b81526004018080602001828103825260248152602001806118b56024913960400191505060405180910390fd5b6001600160a01b038516600090815260026020526040812054610e6c908590611306565b90506000610e8282610e7d89610c83565b61141a565b6001600160a01b03881660009081526004602052604081205491925090610eaa90839061141a565b905084821015610f01576040805162461bcd60e51b815260206004820152601d60248201527f53696d706c65537761703a2063616e6e6f74207061792063616c6c6572000000604482015290519081900360640190fd5b8015610f54576001600160a01b038816600090815260046020526040902054610f2a9082611306565b6001600160a01b038916600090815260046020526040902055600554610f509082611306565b6005555b6001600160a01b038816600090815260026020526040902054610f7790836112a5565b6001600160a01b038916600090815260026020526040902055600354610f9d90836112a5565b6003556001546001600160a01b031663a9059cbb88610fbc8589611306565b6040518363ffffffff1660e01b815260040180836001600160a01b0316815260200182815260200192505050602060405180830381600087803b15801561100257600080fd5b505af1158015611016573d6000803e3d6000fd5b505050506040513d602081101561102c57600080fd5b50516110695760405162461bcd60e51b815260040180806020018281038252602781526020018061183e6027913960400191505060405180910390fd5b841561112a576001546040805163a9059cbb60e01b81523360048201526024810188905290516001600160a01b039092169163a9059cbb916044808201926020929091908290030181600087803b1580156110c357600080fd5b505af11580156110d7573d6000803e3d6000fd5b505050506040513d60208110156110ed57600080fd5b505161112a5760405162461bcd60e51b815260040180806020018281038252602781526020018061183e6027913960400191505060405180910390fd5b6040805183815260208101889052808201879052905133916001600160a01b038a811692908c16917f950494fc3642fae5221b6c32e0e45765c95ebb382a04a71b160db0843e74c99f919081900360600190a48183146111c1576006805460ff60a01b1916600160a01b1790556040517f3f4449c047e11092ec54dc0751b6b4817a9162745de856c893a26e611d18ffc490600090a15b5050505050505050565b604080517f7d824962dd0f01520922ea1766c987b1db570cd5db90bdba5ccf5e32060795026020808301919091526001600160a01b0397881682840152958716606082015260808101949094529190941660a083015260c0808301949094528051808303909401845260e09091019052815191012090565b600080611256611251611430565b61148a565b84604051602001808061190160f01b8152506002018381526020018281526020019250505060405160208183030381529060405280519060200120905061129d81846114fd565b949350505050565b6000828201838110156112ff576040805162461bcd60e51b815260206004820152601b60248201527f536166654d6174683a206164646974696f6e206f766572666c6f770000000000604482015290519081900360640190fd5b9392505050565b60006112ff83836040518060400160405280601e81526020017f536166654d6174683a207375627472616374696f6e206f766572666c6f7700008152506116e8565b604080517fe95f353750f192082df064ca5142d3a2d6f0bef0f3ffad66d80d8af86b7a749a6020808301919091526001600160a01b03958616828401529390941660608501526080808501929092528051808503909201825260a0909301909252815191012090565b604080517f48ebe6deff4a5ee645c01506a026031e2a945d6f41f1f4e5098ad65347492c126020808301919091526001600160a01b03958616828401529390941660608501526080808501929092528051808503909201825260a0909301909252815191012090565b600081831061142957816112ff565b5090919050565b61143861177f565b506040805160a081018252600a6060820190815269436865717565626f6f6b60b01b608083015281528151808301835260038152620312e360ec1b602082810191909152820152469181019190915290565b805180516020918201208183015180519083012060409384015184517fc2f8787176b8ac6bf7215b4adcc1e069bf4ab82d9ab1df05a57a91d425935b6e818601528086019390935260608301919091526080808301919091528351808303909101815260a0909101909252815191012090565b60008151604114611555576040805162461bcd60e51b815260206004820152601f60248201527f45434453413a20696e76616c6964207369676e6174757265206c656e67746800604482015290519081900360640190fd5b60208201516040830151606084015160001a7f7fffffffffffffffffffffffffffffff5d576e7357a4501ddfe92f46681b20a08211156115c65760405162461bcd60e51b81526004018080602001828103825260228152602001806117d56022913960400191505060405180910390fd5b8060ff16601b141580156115de57508060ff16601c14155b1561161a5760405162461bcd60e51b81526004018080602001828103825260228152602001806117f76022913960400191505060405180910390fd5b600060018783868660405160008152602001604052604051808581526020018460ff1681526020018381526020018281526020019450505050506020604051602081039080840390855afa158015611676573d6000803e3d6000fd5b5050604051601f1901519150506001600160a01b0381166116de576040805162461bcd60e51b815260206004820152601860248201527f45434453413a20696e76616c6964207369676e61747572650000000000000000604482015290519081900360640190fd5b9695505050505050565b600081848411156117775760405162461bcd60e51b81526004018080602001828103825283818151815260200191508051906020019080838360005b8381101561173c578181015183820152602001611724565b50505050905090810190601f1680156117695780820380516001836020036101000a031916815260200191505b509250505060405180910390fd5b505050900390565b6040518060600160405280606081526020016060815260200160008152509056fe53696d706c65537761703a2068617264206465706f7369742063616e6e6f74206265206d6f7265207468616e2062616c616e636545434453413a20696e76616c6964207369676e6174757265202773272076616c756545434453413a20696e76616c6964207369676e6174757265202776272076616c756553696d706c65537761703a206465706f736974206e6f74207965742074696d6564206f757453696d706c65537761703a2053696d706c65537761703a207472616e73666572206661696c656453696d706c65537761703a20696e76616c69642062656e6566696369617279207369676e617475726553696d706c65537761703a2068617264206465706f736974206e6f742073756666696369656e7453696d706c65537761703a20696e76616c696420697373756572207369676e617475726553696d706c65537761703a206c697175696442616c616e6365206e6f742073756666696369656e74a26469706673582212208a981966d096f1271490a970697020175e844416a120c1d26a5a037c4c131c0a64736f6c634300060c0033a26469706673582212209a94008a4a4e392cdf5dbd8b34e0fdebbb44558d5260237f748e5d8c552701e064736f6c634300060c0033"

// DeploySimpleSwapFactory deploys a new Ethereum contract, binding an instance of SimpleSwapFactory to it.
func DeploySimpleSwapFactory(auth *bind.TransactOpts, backend bind.ContractBackend, _ERC20Address common.Address) (common.Address, *types.Transaction, *SimpleSwapFactory, error) {
	parsed, err := abi.JSON(strings.NewReader(SimpleSwapFactoryABI))
	if err != nil {
		return common.Address{}, nil, nil, err
	}

	address, tx, contract, err := bind.DeployContract(auth, parsed, common.FromHex(SimpleSwapFactoryBin), backend, _ERC20Address)
	if err != nil {
		return common.Address{}, nil, nil, err
	}
	return address, tx, &SimpleSwapFactory{SimpleSwapFactoryCaller: SimpleSwapFactoryCaller{contract: contract}, SimpleSwapFactoryTransactor: SimpleSwapFactoryTransactor{contract: contract}, SimpleSwapFactoryFilterer: SimpleSwapFactoryFilterer{contract: contract}}, nil
}

// SimpleSwapFactory is an auto generated Go binding around an Ethereum contract.
type SimpleSwapFactory struct {
	SimpleSwapFactoryCaller     // Read-only binding to the contract
	SimpleSwapFactoryTransactor // Write-only binding to the contract
	SimpleSwapFactoryFilterer   // Log filterer for contract events
}

// SimpleSwapFactoryCaller is an auto generated read-only Go binding around an Ethereum contract.
type SimpleSwapFactoryCaller struct {
	contract *bind.BoundContract // Generic contract wrapper for the low level calls
}

// SimpleSwapFactoryTransactor is an auto generated write-only Go binding around an Ethereum contract.
type SimpleSwapFactoryTransactor struct {
	contract *bind.BoundContract // Generic contract wrapper for the low level calls
}

// SimpleSwapFactoryFilterer is an auto generated log filtering Go binding around an Ethereum contract events.
type SimpleSwapFactoryFilterer struct {
	contract *bind.BoundContract // Generic contract wrapper for the low level calls
}

// SimpleSwapFactorySession is an auto generated Go binding around an Ethereum contract,
// with pre-set call and transact options.
type SimpleSwapFactorySession struct {
	Contract     *SimpleSwapFactory // Generic contract binding to set the session for
	CallOpts     bind.CallOpts      // Call options to use throughout this session
	TransactOpts bind.TransactOpts  // Transaction auth options to use throughout this session
}

// SimpleSwapFactoryCallerSession is an auto generated read-only Go binding around an Ethereum contract,
// with pre-set call options.
type SimpleSwapFactoryCallerSession struct {
	Contract *SimpleSwapFactoryCaller // Generic contract caller binding to set the session for
	CallOpts bind.CallOpts            // Call options to use throughout this session
}

// SimpleSwapFactoryTransactorSession is an auto generated write-only Go binding around an Ethereum contract,
// with pre-set transact options.
type SimpleSwapFactoryTransactorSession struct {
	Contract     *SimpleSwapFactoryTransactor // Generic contract transactor binding to set the session for
	TransactOpts bind.TransactOpts            // Transaction auth options to use throughout this session
}

// SimpleSwapFactoryRaw is an auto generated low-level Go binding around an Ethereum contract.
type SimpleSwapFactoryRaw struct {
	Contract *SimpleSwapFactory // Generic contract binding to access the raw methods on
}

// SimpleSwapFactoryCallerRaw is an auto generated low-level read-only Go binding around an Ethereum contract.
type SimpleSwapFactoryCallerRaw struct {
	Contract *SimpleSwapFactoryCaller // Generic read-only contract binding to access the raw methods on
}

// SimpleSwapFactoryTransactorRaw is an auto generated low-level write-only Go binding around an Ethereum contract.
type SimpleSwapFactoryTransactorRaw struct {
	Contract *SimpleSwapFactoryTransactor // Generic write-only contract binding to access the raw methods on
}

// NewSimpleSwapFactory creates a new instance of SimpleSwapFactory, bound to a specific deployed contract.
func NewSimpleSwapFactory(address common.Address, backend bind.ContractBackend) (*SimpleSwapFactory, error) {
	contract, err := bindSimpleSwapFactory(address, backend, backend, backend)
	if err != nil {
		return nil, err
	}
	return &SimpleSwapFactory{SimpleSwapFactoryCaller: SimpleSwapFactoryCaller{contract: contract}, SimpleSwapFactoryTransactor: SimpleSwapFactoryTransactor{contract: contract}, SimpleSwapFactoryFilterer: SimpleSwapFactoryFilterer{contract: contract}}, nil
}

// NewSimpleSwapFactoryCaller creates a new read-only instance of SimpleSwapFactory, bound to a specific deployed contract.
func NewSimpleSwapFactoryCaller(address common.Address, caller bind.ContractCaller) (*SimpleSwapFactoryCaller, error) {
	contract, err := bindSimpleSwapFactory(address, caller, nil, nil)
	if err != nil {
		return nil, err
	}
	return &SimpleSwapFactoryCaller{contract: contract}, nil
}

// NewSimpleSwapFactoryTransactor creates a new write-only instance of SimpleSwapFactory, bound to a specific deployed contract.
func NewSimpleSwapFactoryTransactor(address common.Address, transactor bind.ContractTransactor) (*SimpleSwapFactoryTransactor, error) {
	contract, err := bindSimpleSwapFactory(address, nil, transactor, nil)
	if err != nil {
		return nil, err
	}
	return &SimpleSwapFactoryTransactor{contract: contract}, nil
}

// NewSimpleSwapFactoryFilterer creates a new log filterer instance of SimpleSwapFactory, bound to a specific deployed contract.
func NewSimpleSwapFactoryFilterer(address common.Address, filterer bind.ContractFilterer) (*SimpleSwapFactoryFilterer, error) {
	contract, err := bindSimpleSwapFactory(address, nil, nil, filterer)
	if err != nil {
		return nil, err
	}
	return &SimpleSwapFactoryFilterer{contract: contract}, nil
}

// bindSimpleSwapFactory binds a generic wrapper to an already deployed contract.
func bindSimpleSwapFactory(address common.Address, caller bind.ContractCaller, transactor bind.ContractTransactor, filterer bind.ContractFilterer) (*bind.BoundContract, error) {
	parsed, err := abi.JSON(strings.NewReader(SimpleSwapFactoryABI))
	if err != nil {
		return nil, err
	}
	return bind.NewBoundContract(address, parsed, caller, transactor, filterer), nil
}

// Call invokes the (constant) contract method with params as input values and
// sets the output to result. The result type might be a single field for simple
// returns, a slice of interfaces for anonymous returns and a struct for named
// returns.
func (_SimpleSwapFactory *SimpleSwapFactoryRaw) Call(opts *bind.CallOpts, result *[]interface{}, method string, params ...interface{}) error {
	return _SimpleSwapFactory.Contract.SimpleSwapFactoryCaller.contract.Call(opts, result, method, params...)
}

// Transfer initiates a plain transaction to move funds to the contract, calling
// its default method if one is available.
func (_SimpleSwapFactory *SimpleSwapFactoryRaw) Transfer(opts *bind.TransactOpts) (*types.Transaction, error) {
	return _SimpleSwapFactory.Contract.SimpleSwapFactoryTransactor.contract.Transfer(opts)
}

// Transact invokes the (paid) contract method with params as input values.
func (_SimpleSwapFactory *SimpleSwapFactoryRaw) Transact(opts *bind.TransactOpts, method string, params ...interface{}) (*types.Transaction, error) {
	return _SimpleSwapFactory.Contract.SimpleSwapFactoryTransactor.contract.Transact(opts, method, params...)
}

// Call invokes the (constant) contract method with params as input values and
// sets the output to result. The result type might be a single field for simple
// returns, a slice of interfaces for anonymous returns and a struct for named
// returns.
func (_SimpleSwapFactory *SimpleSwapFactoryCallerRaw) Call(opts *bind.CallOpts, result *[]interface{}, method string, params ...interface{}) error {
	return _SimpleSwapFactory.Contract.contract.Call(opts, result, method, params...)
}

// Transfer initiates a plain transaction to move funds to the contract, calling
// its default method if one is available.
func (_SimpleSwapFactory *SimpleSwapFactoryTransactorRaw) Transfer(opts *bind.TransactOpts) (*types.Transaction, error) {
	return _SimpleSwapFactory.Contract.contract.Transfer(opts)
}

// Transact invokes the (paid) contract method with params as input values.
func (_SimpleSwapFactory *SimpleSwapFactoryTransactorRaw) Transact(opts *bind.TransactOpts, method string, params ...interface{}) (*types.Transaction, error) {
	return _SimpleSwapFactory.Contract.contract.Transact(opts, method, params...)
}

// ERC20Address is a free data retrieval call binding the contract method 0xa6021ace.
//
// Solidity: function ERC20Address() view returns(address)
func (_SimpleSwapFactory *SimpleSwapFactoryCaller) ERC20Address(opts *bind.CallOpts) (common.Address, error) {
	var out []interface{}
	err := _SimpleSwapFactory.contract.Call(opts, &out, "ERC20Address")

	if err != nil {
		return *new(common.Address), err
	}

	out0 := *abi.ConvertType(out[0], new(common.Address)).(*common.Address)

	return out0, err

}

// ERC20Address is a free data retrieval call binding the contract method 0xa6021ace.
//
// Solidity: function ERC20Address() view returns(address)
func (_SimpleSwapFactory *SimpleSwapFactorySession) ERC20Address() (common.Address, error) {
	return _SimpleSwapFactory.Contract.ERC20Address(&_SimpleSwapFactory.CallOpts)
}

// ERC20Address is a free data retrieval call binding the contract method 0xa6021ace.
//
// Solidity: function ERC20Address() view returns(address)
func (_SimpleSwapFactory *SimpleSwapFactoryCallerSession) ERC20Address() (common.Address, error) {
	return _SimpleSwapFactory.Contract.ERC20Address(&_SimpleSwapFactory.CallOpts)
}

// DeployedContracts is a free data retrieval call binding the contract method 0xc70242ad.
//
// Solidity: function deployedContracts(address ) view returns(bool)
func (_SimpleSwapFactory *SimpleSwapFactoryCaller) DeployedContracts(opts *bind.CallOpts, arg0 common.Address) (bool, error) {
	var out []interface{}
	err := _SimpleSwapFactory.contract.Call(opts, &out, "deployedContracts", arg0)

	if err != nil {
		return *new(bool), err
	}

	out0 := *abi.ConvertType(out[0], new(bool)).(*bool)

	return out0, err

}

// DeployedContracts is a free data retrieval call binding the contract method 0xc70242ad.
//
// Solidity: function deployedContracts(address ) view returns(bool)
func (_SimpleSwapFactory *SimpleSwapFactorySession) DeployedContracts(arg0 common.Address) (bool, error) {
	return _SimpleSwapFactory.Contract.DeployedContracts(&_SimpleSwapFactory.CallOpts, arg0)
}

// DeployedContracts is a free data retrieval call binding the contract method 0xc70242ad.
//
// Solidity: function deployedContracts(address ) view returns(bool)
func (_SimpleSwapFactory *SimpleSwapFactoryCallerSession) DeployedContracts(arg0 common.Address) (bool, error) {
	return _SimpleSwapFactory.Contract.DeployedContracts(&_SimpleSwapFactory.CallOpts, arg0)
}

// DeploySimpleSwap is a paid mutator transaction binding the contract method 0x576d7271.
//
// Solidity: function deploySimpleSwap(address issuer, uint256 defaultHardDepositTimeoutDuration) returns(address)
func (_SimpleSwapFactory *SimpleSwapFactoryTransactor) DeploySimpleSwap(opts *bind.TransactOpts, issuer common.Address, defaultHardDepositTimeoutDuration *big.Int) (*types.Transaction, error) {
	return _SimpleSwapFactory.contract.Transact(opts, "deploySimpleSwap", issuer, defaultHardDepositTimeoutDuration)
}

// DeploySimpleSwap is a paid mutator transaction binding the contract method 0x576d7271.
//
// Solidity: function deploySimpleSwap(address issuer, uint256 defaultHardDepositTimeoutDuration) returns(address)
func (_SimpleSwapFactory *SimpleSwapFactorySession) DeploySimpleSwap(issuer common.Address, defaultHardDepositTimeoutDuration *big.Int) (*types.Transaction, error) {
	return _SimpleSwapFactory.Contract.DeploySimpleSwap(&_SimpleSwapFactory.TransactOpts, issuer, defaultHardDepositTimeoutDuration)
}

// DeploySimpleSwap is a paid mutator transaction binding the contract method 0x576d7271.
//
// Solidity: function deploySimpleSwap(address issuer, uint256 defaultHardDepositTimeoutDuration) returns(address)
func (_SimpleSwapFactory *SimpleSwapFactoryTransactorSession) DeploySimpleSwap(issuer common.Address, defaultHardDepositTimeoutDuration *big.Int) (*types.Transaction, error) {
	return _SimpleSwapFactory.Contract.DeploySimpleSwap(&_SimpleSwapFactory.TransactOpts, issuer, defaultHardDepositTimeoutDuration)
}

// SimpleSwapFactorySimpleSwapDeployedIterator is returned from FilterSimpleSwapDeployed and is used to iterate over the raw logs and unpacked data for SimpleSwapDeployed events raised by the SimpleSwapFactory contract.
type SimpleSwapFactorySimpleSwapDeployedIterator struct {
	Event *SimpleSwapFactorySimpleSwapDeployed // Event containing the contract specifics and raw log

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
func (it *SimpleSwapFactorySimpleSwapDeployedIterator) Next() bool {
	// If the iterator failed, stop iterating
	if it.fail != nil {
		return false
	}
	// If the iterator completed, deliver directly whatever's available
	if it.done {
		select {
		case log := <-it.logs:
			it.Event = new(SimpleSwapFactorySimpleSwapDeployed)
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
		it.Event = new(SimpleSwapFactorySimpleSwapDeployed)
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
func (it *SimpleSwapFactorySimpleSwapDeployedIterator) Error() error {
	return it.fail
}

// Close terminates the iteration process, releasing any pending underlying
// resources.
func (it *SimpleSwapFactorySimpleSwapDeployedIterator) Close() error {
	it.sub.Unsubscribe()
	return nil
}

// SimpleSwapFactorySimpleSwapDeployed represents a SimpleSwapDeployed event raised by the SimpleSwapFactory contract.
type SimpleSwapFactorySimpleSwapDeployed struct {
	ContractAddress common.Address
	Raw             types.Log // Blockchain specific contextual infos
}

// FilterSimpleSwapDeployed is a free log retrieval operation binding the contract event 0xc0ffc525a1c7689549d7f79b49eca900e61ac49b43d977f680bcc3b36224c004.
//
// Solidity: event SimpleSwapDeployed(address contractAddress)
func (_SimpleSwapFactory *SimpleSwapFactoryFilterer) FilterSimpleSwapDeployed(opts *bind.FilterOpts) (*SimpleSwapFactorySimpleSwapDeployedIterator, error) {

	logs, sub, err := _SimpleSwapFactory.contract.FilterLogs(opts, "SimpleSwapDeployed")
	if err != nil {
		return nil, err
	}
	return &SimpleSwapFactorySimpleSwapDeployedIterator{contract: _SimpleSwapFactory.contract, event: "SimpleSwapDeployed", logs: logs, sub: sub}, nil
}

// WatchSimpleSwapDeployed is a free log subscription operation binding the contract event 0xc0ffc525a1c7689549d7f79b49eca900e61ac49b43d977f680bcc3b36224c004.
//
// Solidity: event SimpleSwapDeployed(address contractAddress)
func (_SimpleSwapFactory *SimpleSwapFactoryFilterer) WatchSimpleSwapDeployed(opts *bind.WatchOpts, sink chan<- *SimpleSwapFactorySimpleSwapDeployed) (event.Subscription, error) {

	logs, sub, err := _SimpleSwapFactory.contract.WatchLogs(opts, "SimpleSwapDeployed")
	if err != nil {
		return nil, err
	}
	return event.NewSubscription(func(quit <-chan struct{}) error {
		defer sub.Unsubscribe()
		for {
			select {
			case log := <-logs:
				// New log arrived, parse the event and forward to the user
				event := new(SimpleSwapFactorySimpleSwapDeployed)
				if err := _SimpleSwapFactory.contract.UnpackLog(event, "SimpleSwapDeployed", log); err != nil {
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

// ParseSimpleSwapDeployed is a log parse operation binding the contract event 0xc0ffc525a1c7689549d7f79b49eca900e61ac49b43d977f680bcc3b36224c004.
//
// Solidity: event SimpleSwapDeployed(address contractAddress)
func (_SimpleSwapFactory *SimpleSwapFactoryFilterer) ParseSimpleSwapDeployed(log types.Log) (*SimpleSwapFactorySimpleSwapDeployed, error) {
	event := new(SimpleSwapFactorySimpleSwapDeployed)
	if err := _SimpleSwapFactory.contract.UnpackLog(event, "SimpleSwapDeployed", log); err != nil {
		return nil, err
	}
	event.Raw = log
	return event, nil
}
