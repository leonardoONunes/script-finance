import fs from "fs";
import path from "path";
import { parse } from "csv-parse";

const testePath = path.join(__dirname, "..", "teste.csv");
const correctedPath = path.join(__dirname, "..", "corrected-version-1_0.csv");

function readCsv(filePath: string, delimiter: string): Promise<any[]> {
  return new Promise((resolve, reject) => {
    const results: any[] = [];
    const stream = fs.createReadStream(filePath, { encoding: "utf8" });
    const parser = parse({
      columns: true,
      skip_empty_lines: true,
      trim: true,
      delimiter,
      relax_quotes: true,
      relax_column_count: true,
      bom: true,
    });
    stream
      .pipe(parser)
      .on("data", (row: any) => results.push(row))
      .on("end", () => resolve(results))
      .on("error", reject);
    stream.on("error", reject);
  });
}

// Colunas que NÃO estão em notação científica no teste.csv, boas para usar como chave
function makeKey(row: any): string {
  return `${row.cpf}|${row.installmentId}|${row.dueDate}|${row.settlementId}`;
}

// Colunas comparáveis (sem notação científica em teste.csv)
const comparableColumns = [
  "ies",
  "clientId",
  "cpf",
  "name",
  "installmentId",
  "settlementId",
  "product",
  "idealCollector",
  "realCollector",
  "settlementType",
  "paymentMethod",
  "dueDate",
  "settlementDate",
  "principalAmount",
  "chargeAmount",
  "interestAmount",
  "fineAmount",
  "discountAmount",
  "receivedAmount",
  "paymentPercentage",
  "settlementStatus",
  "feePercentage",
  "feeValue",
];

// Colunas em notação científica no teste.csv — precisam de normalização especial
const scientificColumns = [
  "idIes",
  "principiaInstallmentId",
  "paymentId",
];

function normalizeScientific(val: string): string {
  if (!val || val === "null" || val === "") return "";
  const num = parseFloat(val);
  if (!isNaN(num)) return num.toLocaleString("fullwide", { useGrouping: false });
  return val;
}

function normalizeVal(val: string): string {
  if (!val || val === "null") return "";
  let v = val.trim();
  // Remove " 00:00:00" de datas
  v = v.replace(/ 00:00:00$/, "");
  // Normaliza "05/02/2026" → "2026-02-05"
  const brDate = v.match(/^(\d{2})\/(\d{2})\/(\d{4})$/);
  if (brDate) v = `${brDate[3]}-${brDate[2]}-${brDate[1]}`;
  return v;
}

async function main() {
  console.log("Lendo teste.csv...");
  const testeRows = await readCsv(testePath, ";");
  console.log(`  ${testeRows.length} registros\n`);

  console.log("Lendo corrected-version-1_0.csv...");
  const correctedRows = await readCsv(correctedPath, ",");
  console.log(`  ${correctedRows.length} registros\n`);

  // Indexar corrected por chave
  console.log("Indexando corrected por chave (cpf|installmentId|dueDate|settlementId)...\n");
  const correctedMap = new Map<string, { row: any; idx: number }[]>();
  for (let i = 0; i < correctedRows.length; i++) {
    const key = makeKey(correctedRows[i]);
    if (!correctedMap.has(key)) correctedMap.set(key, []);
    correctedMap.get(key)!.push({ row: correctedRows[i], idx: i + 2 });
  }

  let matched = 0;
  let notFound = 0;
  let fullyEqual = 0;
  let withDiffs = 0;

  const diffCountByColumn: Record<string, number> = {};
  for (const col of [...comparableColumns, ...scientificColumns]) {
    diffCountByColumn[col] = 0;
  }

  const exampleDiffs: {
    testeRow: number;
    correctedRow: number;
    key: string;
    diffs: { col: string; teste: string; corrected: string }[];
  }[] = [];

  const notFoundKeys: string[] = [];
  const usedCorrectedIdxs = new Set<number>();

  for (let i = 0; i < testeRows.length; i++) {
    const testeRow = testeRows[i];
    const key = makeKey(testeRow);
    const candidates = correctedMap.get(key);

    if (!candidates || candidates.length === 0) {
      notFound++;
      if (notFoundKeys.length < 10) notFoundKeys.push(key);
      continue;
    }

    // Pegar o primeiro candidato ainda não usado
    let match: { row: any; idx: number } | null = null;
    for (const c of candidates) {
      if (!usedCorrectedIdxs.has(c.idx)) {
        match = c;
        usedCorrectedIdxs.add(c.idx);
        break;
      }
    }
    if (!match) {
      match = candidates[0];
    }

    matched++;
    const correctedRow = match.row;

    const rowDiffs: { col: string; teste: string; corrected: string }[] = [];

    // Comparar colunas normais
    for (const col of comparableColumns) {
      const tVal = normalizeVal(testeRow[col] ?? "");
      const cVal = normalizeVal(correctedRow[col] ?? "");
      if (tVal !== cVal) {
        diffCountByColumn[col]++;
        rowDiffs.push({ col, teste: testeRow[col] ?? "", corrected: correctedRow[col] ?? "" });
      }
    }

    // Comparar colunas em notação científica
    for (const col of scientificColumns) {
      const tVal = normalizeScientific(testeRow[col] ?? "");
      const cVal = normalizeScientific(correctedRow[col] ?? "");
      if (tVal !== cVal) {
        diffCountByColumn[col]++;
        rowDiffs.push({ col, teste: testeRow[col] ?? "", corrected: correctedRow[col] ?? "" });
      }
    }

    if (rowDiffs.length === 0) {
      fullyEqual++;
    } else {
      withDiffs++;
      if (exampleDiffs.length < 15) {
        exampleDiffs.push({
          testeRow: i + 2,
          correctedRow: match.idx,
          key,
          diffs: rowDiffs,
        });
      }
    }
  }

  // Registros no corrected que não foram usados
  const unmatchedCorrected = correctedRows.length - usedCorrectedIdxs.size;

  console.log("=".repeat(100));
  console.log("RESULTADO DA COMPARAÇÃO REGISTRO A REGISTRO");
  console.log("=".repeat(100));
  console.log(`\n  Registros no teste.csv:              ${testeRows.length}`);
  console.log(`  Registros no corrected-version:       ${correctedRows.length}`);
  console.log(`  Registros casados (pela chave):       ${matched}`);
  console.log(`  Registros do teste NÃO encontrados:   ${notFound}`);
  console.log(`  Registros do corrected sem par:        ${unmatchedCorrected}`);
  console.log(`\n  Dos ${matched} registros casados:`);
  console.log(`    Totalmente iguais:     ${fullyEqual}`);
  console.log(`    Com alguma diferença:   ${withDiffs}`);

  console.log(`\n${"=".repeat(100)}`);
  console.log("DIFERENÇAS POR COLUNA (nos registros casados)");
  console.log("=".repeat(100));
  const sortedCols = Object.entries(diffCountByColumn).sort((a, b) => b[1] - a[1]);
  for (const [col, count] of sortedCols) {
    const pct = ((1 - count / matched) * 100).toFixed(2);
    const bar = count > 0 ? `  ** ${count} diffs **` : "  OK";
    console.log(`  ${col.padEnd(30)} ${String(count).padStart(8)} diffs   (${pct}% match)${bar}`);
  }

  if (exampleDiffs.length > 0) {
    console.log(`\n${"=".repeat(100)}`);
    console.log("EXEMPLOS DE REGISTROS COM DIFERENÇAS");
    console.log("=".repeat(100));
    for (const ex of exampleDiffs) {
      console.log(`\n  teste.csv linha ${ex.testeRow} ↔ corrected linha ${ex.correctedRow}`);
      console.log(`  Chave: ${ex.key}`);
      for (const d of ex.diffs) {
        console.log(`    ${d.col}: teste="${d.teste}" vs corrected="${d.corrected}"`);
      }
    }
  }

  if (notFoundKeys.length > 0) {
    console.log(`\n${"=".repeat(100)}`);
    console.log("EXEMPLOS DE REGISTROS DO TESTE NÃO ENCONTRADOS NO CORRECTED");
    console.log("=".repeat(100));
    for (const k of notFoundKeys) {
      console.log(`  Chave: ${k}`);
    }
  }
}

main().catch(console.error);
