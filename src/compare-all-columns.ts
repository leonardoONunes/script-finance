import fs from "fs";
import path from "path";
import { parse } from "csv-parse";

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

/**
 * Normaliza um valor numérico formatado em pt-BR (ex: "1.573" ou "55,05")
 * para formato padrão (ex: "1573" ou "55.05") para comparação justa.
 */
function normalizeNumber(val: string): string {
  if (!val || val === "" || val === "null") return val;
  // Remove aspas
  let v = val.replace(/"/g, "").trim();
  // Se tem ponto como separador de milhar e vírgula como decimal: "1.179,75"
  if (/^\d{1,3}(\.\d{3})*(,\d+)?$/.test(v)) {
    v = v.replace(/\./g, "").replace(",", ".");
  }
  // Se tem apenas vírgula como decimal: "55,05"
  else if (/^\d+(,\d+)$/.test(v)) {
    v = v.replace(",", ".");
  }
  // Remove trailing zeros desnecessários: "700.0" → "700", "1179.75" → "1179.75"
  const num = parseFloat(v);
  if (!isNaN(num)) return String(num);
  return v;
}

/**
 * Normaliza datas para comparação.
 * "2026-01-23 00:00:00" → "2026-01-23"
 * "23/1/2026, 00:00" → "2026-01-23"
 * "2026-02-05 00:00:00" → "2026-02-05"
 * "05/02/2026" → "2026-02-05"
 */
function normalizeDate(val: string): string {
  if (!val || val === "" || val === "null") return val;
  let v = val.replace(/"/g, "").trim();

  // "2026-01-23 00:00:00" → "2026-01-23"
  const isoMatch = v.match(/^(\d{4}-\d{2}-\d{2})/);
  if (isoMatch) return isoMatch[1];

  // "23/1/2026, 00:00" or "05/02/2026"
  const brMatch = v.match(/^(\d{1,2})\/(\d{1,2})\/(\d{4})/);
  if (brMatch) {
    const day = brMatch[1].padStart(2, "0");
    const month = brMatch[2].padStart(2, "0");
    const year = brMatch[3];
    return `${year}-${month}-${day}`;
  }

  return v;
}

function normalizeId(val: string): string {
  if (!val || val === "" || val === "null") return val;
  return val.replace(/\./g, "").replace(/"/g, "").trim();
}

function normalizeGeneral(val: string): string {
  if (!val) return "";
  return val.replace(/"/g, "").trim().toLowerCase();
}

type ColumnMapping = {
  original: string;
  warehouse: string;
  normalizer: "id" | "number" | "date" | "text";
};

const columnMappings: ColumnMapping[] = [
  { original: "ies", warehouse: "ies_warehouse", normalizer: "text" },
  { original: "idIes", warehouse: "ies_id_warehouse", normalizer: "id" },
  { original: "clientId", warehouse: "cliente_id_warehouse", normalizer: "id" },
  { original: "cpf", warehouse: "cpf_warehouse", normalizer: "text" },
  { original: "name", warehouse: "nome_warehouse", normalizer: "text" },
  { original: "installmentId", warehouse: "parcela_ies_id_warehouse", normalizer: "id" },
  { original: "principiaInstallmentId", warehouse: "parcela_id_warehouse", normalizer: "id" },
  { original: "settlementId", warehouse: "liquidacao_id_warehouse", normalizer: "id" },
  { original: "paymentId", warehouse: "pagamento_id_warehouse", normalizer: "id" },
  { original: "product", warehouse: "produto_warehouse", normalizer: "text" },
  { original: "idealCollector", warehouse: "cobrador_ideal_warehouse", normalizer: "text" },
  { original: "realCollector", warehouse: "cobrador_real_warehouse", normalizer: "text" },
  { original: "settlementType", warehouse: "tipo_liquidacao_warehouse", normalizer: "text" },
  { original: "paymentMethod", warehouse: "meio_pagamento_warehouse", normalizer: "text" },
  { original: "dueDate", warehouse: "data_vencimento_warehouse", normalizer: "date" },
  { original: "settlementDate", warehouse: "data_liquidacao_warehouse", normalizer: "date" },
  { original: "paymentDate", warehouse: "data_compensacao_warehouse", normalizer: "date" },
  { original: "principalAmount", warehouse: "valor_principal_warehouse", normalizer: "number" },
  { original: "chargeAmount", warehouse: "valor_encargo_warehouse", normalizer: "number" },
  { original: "interestAmount", warehouse: "valor_juros_warehouse", normalizer: "number" },
  { original: "fineAmount", warehouse: "valor_multa_warehouse", normalizer: "number" },
  { original: "discountAmount", warehouse: "valor_desconto_warehouse", normalizer: "number" },
  { original: "receivedAmount", warehouse: "valor_recebido_warehouse", normalizer: "number" },
  { original: "paymentPercentage", warehouse: "porcentagem_pagamento_warehouse", normalizer: "number" },
  { original: "externalPaymentId", warehouse: "pagamento_id_externo_warehouse", normalizer: "id" },
  { original: "settlementStatus", warehouse: "status_liquidacao_warehouse", normalizer: "text" },
];

function normalize(val: string, type: string): string {
  switch (type) {
    case "id": return normalizeId(val);
    case "number": return normalizeNumber(val);
    case "date": return normalizeDate(val);
    case "text": return normalizeGeneral(val);
    default: return val;
  }
}

async function main() {
  console.log("Lendo corrected-version-1_0.csv...");
  const rows = await readCsv(correctedPath, ",");
  console.log(`Total de linhas: ${rows.length}\n`);

  console.log("=" .repeat(110));
  console.log("COMPARAÇÃO: colunas originais (corrected) vs colunas warehouse");
  console.log("=" .repeat(110));

  const summaryLines: string[] = [];

  for (const mapping of columnMappings) {
    let rawMatch = 0;
    let rawMismatch = 0;
    let normalizedMatch = 0;
    let normalizedMismatch = 0;
    let bothEmpty = 0;
    const examples: { row: number; orig: string; wh: string; normOrig: string; normWh: string }[] = [];

    for (let i = 0; i < rows.length; i++) {
      const origVal = (rows[i][mapping.original] ?? "").toString().trim();
      const whVal = (rows[i][mapping.warehouse] ?? "").toString().trim();

      if (origVal === "" && whVal === "") {
        bothEmpty++;
        rawMatch++;
        normalizedMatch++;
        continue;
      }

      // Raw comparison
      if (origVal === whVal) {
        rawMatch++;
      } else {
        rawMismatch++;
      }

      // Normalized comparison
      const normOrig = normalize(origVal, mapping.normalizer);
      const normWh = normalize(whVal, mapping.normalizer);
      if (normOrig === normWh) {
        normalizedMatch++;
      } else {
        normalizedMismatch++;
        if (examples.length < 5) {
          examples.push({ row: i + 2, orig: origVal, wh: whVal, normOrig, normWh });
        }
      }
    }

    const pctNorm = ((normalizedMatch / rows.length) * 100).toFixed(2);
    const status = normalizedMismatch === 0 ? "OK" : `${normalizedMismatch} DIFFS`;

    console.log(`\n--- ${mapping.original} ↔ ${mapping.warehouse} ---`);
    console.log(`  Comparação BRUTA:       iguais=${rawMatch}  diferentes=${rawMismatch}`);
    console.log(`  Comparação NORMALIZADA: iguais=${normalizedMatch}  diferentes=${normalizedMismatch}  (${pctNorm}% match)`);
    if (bothEmpty > 0) console.log(`  Ambos vazios: ${bothEmpty}`);

    if (examples.length > 0) {
      console.log(`  Exemplos de diferenças (normalizado):`);
      for (const e of examples) {
        console.log(`    Linha ${e.row}: original="${e.orig}" (→"${e.normOrig}") vs warehouse="${e.wh}" (→"${e.normWh}")`);
      }
    }

    summaryLines.push(
      `${mapping.original.padEnd(28)} ↔ ${mapping.warehouse.padEnd(38)} | ${status.padEnd(12)} | bruto: ${rawMismatch} diffs | normalizado: ${normalizedMismatch} diffs (${pctNorm}%)`
    );
  }

  console.log("\n\n" + "=" .repeat(110));
  console.log("RESUMO GERAL");
  console.log("=" .repeat(110));
  for (const line of summaryLines) {
    console.log(`  ${line}`);
  }
}

main().catch(console.error);
