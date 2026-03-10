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

async function main() {
  console.log("Lendo teste.csv...");
  const testeRows = await readCsv(testePath, ";");
  console.log(`  -> ${testeRows.length} linhas\n`);

  console.log("Lendo corrected-version-1_0.csv...");
  const correctedRows = await readCsv(correctedPath, ",");
  console.log(`  -> ${correctedRows.length} linhas\n`);

  // 1) Dentro do corrected: comparar clientId vs cliente_id_warehouse
  console.log("=== COMPARAÇÃO INTERNA (corrected): clientId vs cliente_id_warehouse ===");
  let internalMatch = 0;
  let internalMismatch = 0;
  const internalMismatches: { row: number; clientId: string; clienteIdWarehouse: string }[] = [];

  for (let i = 0; i < correctedRows.length; i++) {
    const row = correctedRows[i];
    const clientId = row.clientId?.trim();
    const clienteIdWh = row.cliente_id_warehouse?.trim();

    if (clientId === clienteIdWh) {
      internalMatch++;
    } else {
      internalMismatch++;
      if (internalMismatches.length < 20) {
        internalMismatches.push({ row: i + 2, clientId, clienteIdWarehouse: clienteIdWh });
      }
    }
  }

  console.log(`  Iguais: ${internalMatch}`);
  console.log(`  Diferentes: ${internalMismatch}`);
  if (internalMismatches.length > 0) {
    console.log(`  Primeiros exemplos de diferenças:`);
    for (const m of internalMismatches) {
      console.log(`    Linha ${m.row}: clientId="${m.clientId}" vs cliente_id_warehouse="${m.clienteIdWarehouse}"`);
    }
  }

  // 2) Comparar teste.csv clientId vs corrected clientId (row by row)
  console.log("\n=== COMPARAÇÃO ENTRE ARQUIVOS (row by row): teste.clientId vs corrected.clientId ===");
  const minLen = Math.min(testeRows.length, correctedRows.length);
  let crossMatch = 0;
  let crossMismatch = 0;
  const crossMismatches: { row: number; testeClientId: string; correctedClientId: string }[] = [];

  for (let i = 0; i < minLen; i++) {
    const testeClientId = testeRows[i].clientId?.trim();
    const correctedClientId = correctedRows[i].clientId?.trim();

    if (testeClientId === correctedClientId) {
      crossMatch++;
    } else {
      crossMismatch++;
      if (crossMismatches.length < 20) {
        crossMismatches.push({ row: i + 2, testeClientId, correctedClientId });
      }
    }
  }

  console.log(`  Linhas comparadas: ${minLen}`);
  console.log(`  Iguais: ${crossMatch}`);
  console.log(`  Diferentes: ${crossMismatch}`);
  if (crossMismatches.length > 0) {
    console.log(`  Primeiros exemplos de diferenças:`);
    for (const m of crossMismatches) {
      console.log(`    Linha ${m.row}: teste="${m.testeClientId}" vs corrected="${m.correctedClientId}"`);
    }
  }

  if (testeRows.length !== correctedRows.length) {
    console.log(`\n  ⚠ Diferença de ${Math.abs(testeRows.length - correctedRows.length)} linhas entre os arquivos`);
  }

  // 3) Comparar conjuntos de clientIds únicos
  console.log("\n=== COMPARAÇÃO DE CONJUNTOS DE clientId ÚNICOS ===");
  const testeClientIds = new Set(testeRows.map((r) => r.clientId?.trim()));
  const correctedClientIds = new Set(correctedRows.map((r) => r.clientId?.trim()));
  const correctedWhClientIds = new Set(correctedRows.map((r) => r.cliente_id_warehouse?.trim()));

  console.log(`  ClientIds únicos em teste.csv: ${testeClientIds.size}`);
  console.log(`  ClientIds únicos em corrected (clientId): ${correctedClientIds.size}`);
  console.log(`  ClientIds únicos em corrected (cliente_id_warehouse): ${correctedWhClientIds.size}`);

  const onlyInTeste = [...testeClientIds].filter((id) => !correctedClientIds.has(id));
  const onlyInCorrected = [...correctedClientIds].filter((id) => !testeClientIds.has(id));

  console.log(`\n  ClientIds presentes APENAS em teste.csv: ${onlyInTeste.length}`);
  if (onlyInTeste.length > 0 && onlyInTeste.length <= 20) {
    onlyInTeste.forEach((id) => console.log(`    - ${id}`));
  } else if (onlyInTeste.length > 20) {
    onlyInTeste.slice(0, 20).forEach((id) => console.log(`    - ${id}`));
    console.log(`    ... e mais ${onlyInTeste.length - 20}`);
  }

  console.log(`  ClientIds presentes APENAS em corrected: ${onlyInCorrected.length}`);
  if (onlyInCorrected.length > 0 && onlyInCorrected.length <= 20) {
    onlyInCorrected.forEach((id) => console.log(`    - ${id}`));
  } else if (onlyInCorrected.length > 20) {
    onlyInCorrected.slice(0, 20).forEach((id) => console.log(`    - ${id}`));
    console.log(`    ... e mais ${onlyInCorrected.length - 20}`);
  }
}

main().catch(console.error);
