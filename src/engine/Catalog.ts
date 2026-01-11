import { DATA_DIR } from "./Constants";
import type { BunFile } from "bun";
import type { Column } from "./Table";
import { join } from "path";

export interface CatalogData {
    tables: Record<string, Column[]>;
}

export class Catalog {
    private path = join(DATA_DIR, "catalog.json");
    public data: CatalogData = { tables: {} };

    constructor(customPath?: string) {
        if (customPath) this.path = join(DATA_DIR, customPath);
    }

    async init() {
        const fs = await import('node:fs/promises');
        try {
            const content = await fs.readFile(this.path, "utf-8");
            this.data = JSON.parse(content);
        } catch {
            // Defaults
            await this.save();
        }
    }

    async save() {
        const fs = await import('node:fs/promises');
        await fs.writeFile(this.path, JSON.stringify(this.data, null, 2));
    }

    addTable(name: string, columns: Column[]) {
        this.data.tables[name] = columns;
        this.save();
    }

    getTable(name: string): Column[] | undefined {
        return this.data.tables[name];
    }
}
