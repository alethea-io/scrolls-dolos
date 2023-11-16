import type { JsonValue } from "npm:@bufbuild/protobuf";

import * as C from "npm:@dcspark/cardano-multiplatform-lib-nodejs";
import * as UtxoRpc from "npm:@utxorpc-web/cardano-spec";


function bytesToAddress(bytes: Uint8Array){
  const header = bytes[0];
  const address = C.Address.from_bytes(bytes)

  switch (header & 0b11110000) {
    case 0b00000000:
      return address.to_bech32("addr");
    case 0b00010000:
      return address.to_bech32("addr");
    case 0b00100000:
      return address.to_bech32("addr");
    case 0b00110000:
      return address.to_bech32("addr");
    case 0b01000000:
      return address.to_bech32("addr");
    case 0b01010000:
      return address.to_bech32("addr");
    case 0b01100000:
      return address.to_bech32("addr");
    case 0b01110000:
      return address.to_bech32("addr");
    case 0b10000000:
      return address.as_byron()?.to_base58();
    case 0b11100000:
      return address.to_bech32("stake");
    case 0b11110000:
      return address.to_bech32("stake");
  }
}


enum Action {
  Produce = "produce",
  Consume = "consume",
}


function processTxOutput(txOuput: UtxoRpc.TxOutput, action: Action) {
  const address = bytesToAddress(txOuput.address);

  let value;
  switch(action){
    case Action.Consume:
      value = txOuput.coin * -1n;
      break;
    case Action.Produce:
      value = txOuput.coin;
      break;
  }

  return {
    command: "PNCounter",
    key: address,
    value: value,
  };
}


export function apply(blockJson: JsonValue) {
  const block = UtxoRpc.Block.fromJson(blockJson);

  const commands = []
  for(const tx of block.body?.tx ?? []){
    for(const txOutput of tx.outputs) {
      const command = processTxOutput(txOutput, Action.Produce);
      commands.push(command);
    }

    for(const txInput of tx.inputs) {
      const txOuput = txInput.asOutput;
      if(txOuput) {
        const command = processTxOutput(txOuput, Action.Consume);
        commands.push(command)
      }
    }
  }

  return commands
}


export function undo(blockJson: JsonValue) {
  const block = UtxoRpc.Block.fromJson(blockJson);

  const commands = []
  for(const tx of block.body?.tx ?? []){
    for(const txOutput of tx.outputs) {
      const command = processTxOutput(txOutput, Action.Consume);
      commands.push(command);
    }

    for(const txInput of tx.inputs) {
      const txOuput = txInput.asOutput;
      if(txOuput) {
        const command = processTxOutput(txOuput, Action.Produce);
        commands.push(command)
      }
    }
  }

  return commands
}