import type { JsonValue } from "npm:@bufbuild/protobuf";
import * as UtxoRpc from "npm:@utxorpc-web/cardano-spec";
import { C } from "./core/mod.ts";

type Delta = {
  key: string;
  value: bigint;
}

type Command = {
  command: string;
  sql: string;
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
    key: stakeAddressString,
    value: value,
  };
}

function processBlock(
  blockJson: JsonValue,
  config: Record<string, string>,
  applyOrUndo: string,
) {
  const block = UtxoRpc.Block.fromJson(blockJson);

  const deltas: Record<string, bigint> = {};
  for (const tx of block.body?.tx ?? []) {
    for (const txOutput of tx.outputs) {
      const action = applyOrUndo == "apply" ? Action.Produce : Action.Consume;
      const delta: Delta | null = processTxOutput(txOutput, action);
      if (delta && delta.key in deltas) {
        deltas[delta.key] += delta.value;
      } else if (delta && !(delta.key in deltas)) {
        deltas[delta.key] = delta.value;
      }
    }

    for (const txInput of tx.inputs) {
      const txOutput = txInput.asOutput;
      if (txOutput) {
        const action = applyOrUndo == "apply" ? Action.Consume : Action.Produce;
        const delta: Delta | null = processTxOutput(txOutput, action);
        if (delta && delta.key in deltas) {
          deltas[delta.key] += delta.value;
        } else if (delta && !(delta.key in deltas)) {
          deltas[delta.key] = delta.value;
        }
      }
    }
  }

  const commands: Command[] = [];
  for (const [key, value] of Object.entries(deltas)) {
    commands.push({
      command: "ExecuteSQL",
      sql: (
        `
        INSERT INTO ${config.table} (address, balance)
        VALUES ('${key}', ${value})
        ON CONFLICT (address) DO UPDATE SET
        balance = ${config.table}.balance + EXCLUDED.balance;
        `
      )
    });
  }

  return commands;
}

export function apply(blockJson: JsonValue, config: Record<string, string>) {
  return processBlock(blockJson, config, "apply");
}

export function undo(blockJson: JsonValue, config: Record<string, string>) {
  return processBlock(blockJson, config, "undo");
}
