import type { CreateStmt, InsertStmt, SelectStmt, DeleteStmt, UpdateStmt, DropStmt, Expr, BeginStmt, CommitStmt } from "./AST";
import { ColumnType } from "./Constants";

export class Parser {
    private pos = 0;
    private tokens: string[] = [];

    constructor(private sql: string) {
        this.tokenize();
    }

    private tokenize() {
        const regex = /'[^']*'|"[^"]*"|\d+(?:\.\d+)?|[a-zA-Z0-9_]+|[=<>!]+|[(),*]/g;
        this.tokens = this.sql.match(regex) || [];
    }

    private peek(): string {
        return this.tokens[this.pos] || "";
    }

    private consume(expect?: string): string {
        const token = this.tokens[this.pos];
        if (!token) {
            throw new Error(`Unexpected End of Input. Expected ${expect || "token"}`);
        }
        if (expect && token.toUpperCase() !== expect.toUpperCase()) {
            throw new Error(`Expected ${expect}, found ${token} at pos ${this.pos}`);
        }
        this.pos++;
        return token;
    }

    parse(): CreateStmt | InsertStmt | SelectStmt | DeleteStmt | UpdateStmt | DropStmt | BeginStmt | CommitStmt {
        const token = this.peek().toUpperCase();
        if (token === 'CREATE') return this.parseCreate();
        if (token === 'INSERT') return this.parseInsert();
        if (token === 'SELECT') return this.parseSelect();
        if (token === 'DELETE') return this.parseDelete();
        if (token === 'UPDATE') return this.parseUpdate();
        if (token === 'DROP') return this.parseDrop();
        if (token === 'BEGIN') {
            this.consume('BEGIN');
            if (this.peek().toUpperCase() === 'TRANSACTION') this.consume('TRANSACTION');
            return { type: 'BEGIN' };
        }
        if (token === 'COMMIT') {
            this.consume('COMMIT');
            return { type: 'COMMIT' };
        }
        throw new Error(`Unknown command: ${token}`);
    }

    private parseDrop(): DropStmt {
        this.consume('DROP');
        this.consume('TABLE');
        const table = this.consume();
        return { type: 'DROP', table };
    }

    private parseCreate(): CreateStmt {
        this.consume('CREATE');
        this.consume('TABLE');
        const table = this.consume();
        this.consume('(');
        const columns = [];
        while (this.peek() !== ')') {
            const name = this.consume();
            const typeStr = this.consume().toUpperCase();
            let type: ColumnType;
            if (typeStr === 'INT') type = ColumnType.INT;
            else if (typeStr === 'STRING') type = ColumnType.STRING;
            else if (typeStr === 'BOOLEAN') type = ColumnType.BOOLEAN;
            else if (typeStr === 'FLOAT') type = ColumnType.FLOAT;
            else throw new Error(`Unknown Type: ${typeStr}`);

            let isPrimary = false;
            if (this.peek().toUpperCase() === 'PRIMARY') {
                this.consume('PRIMARY');
                this.consume('KEY');
                isPrimary = true;
            }

            columns.push({ name, type, isPrimary });
            if (this.peek() === ',') this.consume(',');
        }
        this.consume(')');
        return { type: 'CREATE', table, columns };
    }

    private parseInsert(): InsertStmt {
        this.consume('INSERT');
        this.consume('INTO');
        const table = this.consume();
        this.consume('VALUES');
        this.consume('(');
        const values = [];
        while (this.peek() !== ')') {
            values.push(this.parseLiteral());
            if (this.peek() === ',') this.consume(',');
        }
        this.consume(')');
        return { type: 'INSERT', table, values };
    }

    private parseSelect(): SelectStmt {
        this.consume('SELECT');
        const columns = [];
        if (this.peek() === '*') {
            this.consume('*');
            columns.push('*');
        } else {
            while (this.peek().toUpperCase() !== 'FROM') {
                columns.push(this.consume());
                if (this.peek() === ',') this.consume(',');
            }
        }
        this.consume('FROM');
        const table = this.consume();

        let where: Expr | undefined;
        if (this.peek().toUpperCase() === 'WHERE') {
            this.consume('WHERE');
            where = this.parseExpr();
        }

        let limit: number | undefined;
        if (this.peek().toUpperCase() === 'LIMIT') {
            this.consume('LIMIT');
            limit = parseInt(this.consume());
        }

        return { type: 'SELECT', table, columns, where, limit };
    }

    private parseDelete(): DeleteStmt {
        this.consume('DELETE');
        this.consume('FROM');
        const table = this.consume();
        let where: Expr | undefined;
        if (this.peek().toUpperCase() === 'WHERE') {
            this.consume('WHERE');
            where = this.parseExpr();
        }
        return { type: 'DELETE', table, where };
    }

    private parseUpdate(): UpdateStmt {
        this.consume('UPDATE');
        const table = this.consume();
        this.consume('SET');
        const assignments = [];
        do {
            const column = this.consume();
            this.consume('=');
            const value = this.parseLiteral();
            assignments.push({ column, value });
        } while (this.peek() === ',' && this.consume(','));

        let where: Expr | undefined;
        if (this.peek().toUpperCase() === 'WHERE') {
            this.consume('WHERE');
            where = this.parseExpr();
        }
        return { type: 'UPDATE', table, assignments, where };
    }

    private parseExpr(): Expr {
        return this.parseOr();
    }

    private parseOr(): Expr {
        let left = this.parseAnd();
        while (this.peek().toUpperCase() === 'OR') {
            this.consume('OR');
            const right = this.parseAnd();
            left = { type: 'BINARY', op: 'OR', left, right };
        }
        return left;
    }

    private parseAnd(): Expr {
        let left = this.parseComp();
        while (this.peek().toUpperCase() === 'AND') {
            this.consume('AND');
            const right = this.parseComp();
            left = { type: 'BINARY', op: 'AND', left, right };
        }
        return left;
    }

    private parseComp(): Expr {
        let left = this.parsePrimary();
        while (['=', '>', '<', '!='].includes(this.peek())) {
            const op = this.consume();
            const right = this.parsePrimary();
            left = { type: 'BINARY', op, left, right };
        }
        return left;
    }

    private parsePrimary(): Expr {
        const token = this.peek();
        if (token.startsWith("'") || token.startsWith('"')) {
            this.consume();
            return { type: 'LITERAL', value: token.slice(1, -1) };
        }
        if (!isNaN(parseFloat(token))) {
            this.consume();
            return { type: 'LITERAL', value: parseFloat(token) };
        }
        if (token.toUpperCase() === 'TRUE') { this.consume(); return { type: 'LITERAL', value: true }; }
        if (token.toUpperCase() === 'FALSE') { this.consume(); return { type: 'LITERAL', value: false }; }

        return { type: 'IDENTIFIER', name: this.consume() };
    }

    private parseLiteral(): any {
        const expr = this.parsePrimary();
        if (expr.type !== 'LITERAL') throw new Error("Expected Literal");
        return expr.value;
    }
}
