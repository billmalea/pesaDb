import { ColumnType } from "./Constants";

export interface CreateStmt {
    type: 'CREATE';
    table: string;
    columns: { name: string; type: ColumnType; isPrimary: boolean }[];
}

export interface InsertStmt {
    type: 'INSERT';
    table: string;
    values: any[];
}

export interface SelectStmt {
    type: 'SELECT';
    table: string;
    columns: string[]; // "*" or specific columns
    where?: Expr;
    limit?: number;
}

export interface DeleteStmt {
    type: 'DELETE';
    table: string;
    where?: Expr;
}

export interface UpdateStmt {
    type: 'UPDATE';
    table: string;
    assignments: { column: string; value: any }[];
    where?: Expr;
}

export interface DropStmt {
    type: 'DROP';
    table: string;
}

export interface BeginStmt {
    type: 'BEGIN';
}

export interface CommitStmt {
    type: 'COMMIT';
}

// Expression AST
export type Expr =
    | { type: 'BINARY'; op: string; left: Expr; right: Expr }
    | { type: 'LITERAL'; value: any }
    | { type: 'IDENTIFIER'; name: string };
