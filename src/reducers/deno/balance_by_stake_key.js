export function apply(block) {
  return {
    command: "PNCounter",
    key: "stake_key",
    value: 1
  }
}

export function undo(block) {
  return {
    command: "PNCounter",
    key: "stake_key",
    value: -1
  }
}