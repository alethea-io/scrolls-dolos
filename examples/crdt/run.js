import * as reducers from './reducers/dist/mod.js'

const config = [
  {
    name: "BalanceByAddress",
    config: {
      addressType: "payment",
      prefix: "balance_by_address",
    }
  },
  {
    name: "BalanceByAddress",
    config: {
      addressType: "stake",
      prefix: "balance_by_stake_address",
    }
  },
]

export function apply(blockJson) {
  return reducers.apply(blockJson, config)
}

export function undo(blockJson) {
  return reducers.undo(blockJson, config)
}