def is_self_transfer(wallet: str, from_addr: str, to_addr: str) -> bool:
    return wallet.lower() == from_addr.lower() and wallet.lower() == to_addr.lower()
