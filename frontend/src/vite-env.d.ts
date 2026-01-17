/// <reference types="vite/client" />

declare global {
  // ---------- Pyodide ----------
  type LoadPyodide = (options?: { indexURL?: string }) => Promise<Pyodide>;

  interface PyodideGlobals {
    get: (name: string) => unknown;
    set: (name: string, value: unknown) => void;
  }

  interface Pyodide {
    loadPackage: (packages: string | string[]) => Promise<void>;
    runPythonAsync: (code: string) => Promise<unknown>;
    globals: PyodideGlobals;
  }

  // ---------- sql.js ----------
  interface SqlJsQueryResult {
    columns: string[];
    values: unknown[][];
  }

  interface SqlJsStatement {
    run: (params?: unknown[]) => void;
    free: () => void;
  }

  interface SqlJsDatabase {
    run: (sql: string, params?: unknown[]) => void;
    exec: (sql: string, params?: unknown[]) => SqlJsQueryResult[];
    prepare: (sql: string) => SqlJsStatement;
    close?: () => void;
  }

  interface SqlJsStatic {
    Database: new () => SqlJsDatabase;
  }

  type InitSqlJs = (config?: {
    locateFile?: (file: string) => string;
  }) => Promise<SqlJsStatic>;

  interface Window {
    loadPyodide?: LoadPyodide;
    initSqlJs?: InitSqlJs;
  }
}

export {};
