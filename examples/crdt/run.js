import * as reducers from './reducers/dist/mod.js'

const config = [
  {
    name: "BalanceByAddress",
    config: {
      prefix: "balance_by_address",
    }
  },
  {
    name: "BalanceByStakeAddress",
    config: {
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