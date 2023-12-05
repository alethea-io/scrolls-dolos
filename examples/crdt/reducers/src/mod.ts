import type { JsonValue } from "npm:@bufbuild/protobuf";
import * as BalanceByAddress from "./balance_by_address.ts";
import * as BalanceByStakeKey from "./balance_by_stake_key.ts";

const reducers = [
  BalanceByAddress,
  BalanceByStakeKey,
];

export function apply(blockJson: JsonValue) {
  return reducers.flatMap((reducer) => reducer.apply(blockJson));
}

export function undo(blockJson: JsonValue) {
  return reducers.flatMap((reducer) => reducer.undo(blockJson));
}
