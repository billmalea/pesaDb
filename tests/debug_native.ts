import { NativeWalManager, WalOpType } from "../src/engine/NativeWAL";

async function run() {
    console.log("Debug: Init Native WAL");
    const wal = new NativeWalManager("debug_test");
    await wal.init();

    console.log("Debug: Appending 1 row...");
    const res = await wal.append(1, 100, WalOpType.INSERT, "users", "test_data_payload", false);
    console.log(`Debug: Result 1: ${res}`);

    console.log("Debug: Appending 2nd row...");
    const res2 = await wal.append(2, 101, WalOpType.INSERT, "users", "test_data_payload_2", true); // Sync true
    console.log(`Debug: Result 2: ${res2}`);

    await wal.close();
    console.log("Debug: Done");
}

run();
