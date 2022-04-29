#!/usr/bin/env node --async-stack-traces
require("dotenv").config();
const Web3 = require("web3");
const csv = require("csv-parser");
const fs = require("fs");

let minABI = [
  // transfer
  {
    constant: false,
    inputs: [
      {
        name: "_to",
        type: "address",
      },
      {
        name: "_value",
        type: "uint256",
      },
    ],
    name: "transfer",
    outputs: [
      {
        name: "",
        type: "bool",
      },
    ],
    type: "function",
  },
];

function handlePaymentCSV() {
  const payments = [];

  let res2 = fs
    .createReadStream("payments.csv")
    .pipe(csv())
    .on("data", (data) => payments.push(data))
    .on("end", () => {
      sendAllTransactions(payments);
    });
}

async function sendOneTransaction(
  web3,
  contract,
  toAddress,
  raw_amount,
  transCount
) {
  let fromAddress = web3.eth.accounts.wallet["0"].address;

  let amount = web3.utils.toWei(raw_amount, "ether");
  var gasLimit = 50000;

  let gasPrice = await web3.eth.getGasPrice();

  try {
    payload = {
      from: fromAddress,
      gasPrice: web3.utils.toHex(Math.floor(gasPrice * 1.5)),
      //        nonce: nonce,
      gasLimit: web3.utils.toHex(gasLimit),
    };

    //console.log(payload)

    info = await contract.methods.transfer(toAddress, amount).send(payload);

    console.log(
      `${raw_amount} sent to ${toAddress},  tx:   ${info.transactionHash}`
    );
  } catch (e) {
    console.log({ e });
  }
}

async function sendAllTransactions(payments, privateKey) {
  const WETH = "0x7ceb23fd6bc0add59e62ac25578270cff1b9f619";

  const web3 = new Web3("https://polygon-rpc.com/");
  let contract = new web3.eth.Contract(minABI, WETH);

  web3.eth.accounts.wallet.add(
    web3.eth.accounts.privateKeyToAccount(privateKey)
  );
  let fromAddress = web3.eth.accounts.wallet["0"].address;

  console.log(
    `Sending payments from '${fromAddress}' to ${payments.length} accounts.`
  );

  let transCount = await web3.eth.getTransactionCount(fromAddress);

  for (const payment of payments) {
    try {
      await sendOneTransaction(
        web3,
        contract,
        payment["WALLET"],
        payment["AMOUNT"],
        transCount
      );
      transCount = transCount + 1;
    } catch (err) {
      console.log(err.message);
    }
  }
  return transCount;
}

const send_weth = {
  sendAllTransactions,
};

module.exports = send_weth;
