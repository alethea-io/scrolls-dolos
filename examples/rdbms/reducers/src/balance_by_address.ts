import type { JsonValue } from "npm:@bufbuild/protobuf";
import * as UtxoRpc from "npm:@utxorpc-web/cardano-spec";
import { C } from "./core/mod.ts";

type Command = {
  command: string;
  key: string;
  value: string;
};

enum Action {
  Produce = "produce",
  Consume = "consume",
}

function processTxOutput(txOuput: UtxoRpc.TxOutput, action: Action) {
  const address = C.Address.from_bytes(txOuput.address);

  let addressString = "";

  if (address.as_byron()) {
    // @ts-ignore: checked if address.as_byron() is undefined
    addressString = address.as_byron()?.to_base58(); 
  } else if (address.to_bech32(undefined)) {
    addressString = address.to_bech32(undefined);
  } else {
    const addressHex = Array.from(
      txOuput.address,
      (byte) => byte.toString(16).padStart(2, "0"),
    ).join("");
    throw new Error(`address ${addressHex} could not be parsed!`);
  }

  let value;
  switch (action) {
    case Action.Consume:
      value = -txOuput.coin;
      break;
    case Action.Produce:
      value = txOuput.coin;
      break;
  }

  return {
    command: "PNCounter",
    key: addressString,
    // Must stringify to return bigint to rust
    value: value.toString(),
  };
}

export function apply(blockJson: JsonValue) {
  const block = UtxoRpc.Block.fromJson(blockJson);

  const commands: Command[] = [];
  for (const tx of block.body?.tx ?? []) {
    for (const txOutput of tx.outputs) {
      const command = processTxOutput(txOutput, Action.Produce);
      commands.push(command);
    }

    for (const txInput of tx.inputs) {
      const txOuput = txInput.asOutput;
      if (txOuput) {
        const command = processTxOutput(txOuput, Action.Consume);
        commands.push(command);
      }
    }
  }

  return commands;
}

export function undo(blockJson: JsonValue) {
  const block = UtxoRpc.Block.fromJson(blockJson);

  const commands: Command[] = [];
  for (const tx of block.body?.tx ?? []) {
    for (const txOutput of tx.outputs) {
      const command = processTxOutput(txOutput, Action.Consume);
      commands.push(command);
    }

    for (const txInput of tx.inputs) {
      const txOuput = txInput.asOutput;
      if (txOuput) {
        const command = processTxOutput(txOuput, Action.Produce);
        commands.push(command);
      }
    }
  }

  return commands;
}
