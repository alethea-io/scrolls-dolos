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

  let stakeAddressString = "";

  if (address.as_base()) {
    const network_id = address.network_id();
    const stake_cred = address.as_base()?.stake_cred();

    stakeAddressString = C.RewardAddress
      // @ts-ignore: checked if address.as_base() is undefined
      .new(network_id, stake_cred)
      .to_address()
      .to_bech32(undefined);
  } else {
    return null;
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
    key: stakeAddressString,
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
      if (command) {
        commands.push(command);
      }
    }

    for (const txInput of tx.inputs) {
      const txOuput = txInput.asOutput;
      if (txOuput) {
        const command = processTxOutput(txOuput, Action.Consume);
        if (command) {
          commands.push(command);
        }
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
      if (command) {
        commands.push(command);
      }
    }

    for (const txInput of tx.inputs) {
      const txOuput = txInput.asOutput;
      if (txOuput) {
        const command = processTxOutput(txOuput, Action.Produce);
        if (command) {
          commands.push(command);
        }
      }
    }
  }

  return commands;
}
