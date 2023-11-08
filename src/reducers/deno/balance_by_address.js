export function apply(block) {
  return {
    command: "PNCounter",
    key: "address",
    value: 1
  }
}

export function undo(block) {
  return {
    command: "PNCounter",
    key: "address",
    value: -1
  }
}