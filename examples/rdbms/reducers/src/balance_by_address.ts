import type { JsonValue } from "npm:@bufbuild/protobuf";
import * as UtxoRpc from "npm:@utxorpc-web/cardano-spec";
import { C } from "./core/mod.ts";

enum Method {
  Apply = "apply",
  Undo = "undo",
}

enum Action {
  Produce = "produce",
  Consume = "consume",
}

function processTxOutput(txOuput: UtxoRpc.TxOutput, addressType: string, action: Action) {
  const address = C.Address.from_bytes(txOuput.address);

  let key: string;

  switch (addressType) {
    case "payment":
      if (address.as_byron()) {
        // @ts-ignore: checked if address.as_byron() is undefined
        key = address.as_byron()?.to_base58();
      } else if (address.to_bech32(undefined)) {
        key = address.to_bech32(undefined);
      } else {
        const addressHex = Array.from(
          txOuput.address,
          (byte) => byte.toString(16).padStart(2, "0"),
        ).join("");
        throw new Error(`address "${addressHex}" could not be parsed!`);
      }
      break
    case "stake":
      if (address.as_base()) {
        const network_id = address.network_id();
        const stake_cred = address.as_base()?.stake_cred();
    
        key = C.RewardAddress
          // @ts-ignore: checked if address.as_base() is undefined
          .new(network_id, stake_cred)
          .to_address()
          .to_bech32(undefined);
      } else {
        return null;
      }
      break
    default:
      throw new Error(`address type "${addressType}" not implemented`);
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

  return { key, value };
}

function processBlock(
  blockJson: JsonValue,
  config: Record<string, string>,
  method: Method,
) {
  const block = UtxoRpc.Block.fromJson(blockJson);
  const addressType = config.addressType
  const table = config.table

  const deltas: Record<string, bigint> = {};
  for (const tx of block.body?.tx ?? []) {
    for (const txOutput of tx.outputs) {
      let action: Action;
      switch (method) {
        case Method.Apply:
          action = Action.Produce;
          break;
        case Method.Undo:
          action = Action.Consume;
          break;
      }

      const delta = processTxOutput(txOutput, addressType, action);
      if (delta) {
        if (delta.key in deltas) {
          deltas[delta.key] += delta.value;
        } else {
          deltas[delta.key] = delta.value;
        }
      }
    }

    for (const txInput of tx.inputs) {
      const txOutput = txInput.asOutput;
      if (txOutput) {
        let action: Action;
        switch (method) {
          case Method.Apply:
            action = Action.Consume;
            break;
          case Method.Undo:
            action = Action.Produce;
            break;
        }

        const delta = processTxOutput(txOutput, addressType, action);
        if (delta) {
          if (delta.key in deltas) {
            deltas[delta.key] += delta.value;
          } else {
            deltas[delta.key] = delta.value;
          }
        }
      }
    }
  }

  const keys = Object.keys(deltas);
  const values = Object.values(deltas);
  
  if (keys.length > 0) {
    const inserted = {
      command: "ExecuteSQL",
      sql: `
        INSERT INTO ${table} (address, balance)
        SELECT unnest(ARRAY[${keys.map(key => `'${key}'`).join(",")}]) AS address,
               unnest(ARRAY[${values.join(",")}]) AS balance
        ON CONFLICT (address) DO UPDATE
        SET balance = ${table}.balance + EXCLUDED.balance
      `,
    };
  
    const deleted = {
      command: "ExecuteSQL",
      sql: `
        DELETE FROM ${table}
        WHERE address IN (${keys.map(key => `'${key}'`).join(",")})
          AND balance = 0
      `,
    };    
  
    return [inserted, deleted];

  } else {

    return [];

  }
}

export function apply(blockJson: JsonValue, config: Record<string, string>) {
  return processBlock(blockJson, config, Method.Apply);
}

export function undo(blockJson: JsonValue, config: Record<string, string>) {
  return processBlock(blockJson, config, Method.Undo);
}
