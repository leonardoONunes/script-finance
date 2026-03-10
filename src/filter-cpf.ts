import fs from "fs";
import path from "path";
import { parse } from "csv-parse";

const filePath = path.join(__dirname, "..", "teste.csv");

type CpfData = {
  cpf: string;
  minDueDate: string;
  maxDueDate: string;
};

const cpfMap = new Map<string, { min: string; max: string }>();

const parser = parse({
  columns: true,
  skip_empty_lines: true,
  trim: true,
  delimiter: ";",
  relax_quotes: true,
  bom: true,
});

const stream = fs.createReadStream(filePath, { encoding: "utf8" });

stream
  .pipe(parser)
  .on("data", (row: any) => {
    const cpf = row.cpf;
    const dueDate = row.dueDate;
    if (!cpf || !dueDate) return;

    const existing = cpfMap.get(cpf);
    if (!existing) {
      cpfMap.set(cpf, { min: dueDate, max: dueDate });
    } else {
      if (dueDate < existing.min) existing.min = dueDate;
      if (dueDate > existing.max) existing.max = dueDate;
    }
  })
  .on("end", () => {
    const results: CpfData[] = [];
    for (const [cpf, dates] of cpfMap) {
      results.push({ cpf, minDueDate: dates.min, maxDueDate: dates.max });
    }
    results.sort((a, b) => a.cpf.localeCompare(b.cpf));

    console.log(`Total de CPFs únicos: ${results.length}\n`);
    console.log("cpf;minDueDate;maxDueDate");
    for (const r of results) {
      console.log(`${r.cpf};${r.minDueDate};${r.maxDueDate}`);
    }

    const outputPath = path.join(__dirname, "..", "resultado-cpf.csv");
    const header = "cpf;minDueDate;maxDueDate\n";
    const lines = results.map((r) => `${r.cpf};${r.minDueDate};${r.maxDueDate}`).join("\n");
    fs.writeFileSync(outputPath, header + lines, "utf8");
    console.log(`\nArquivo salvo em: ${outputPath}`);
  })
  .on("error", (err: any) => {
    console.error("Erro ao processar CSV:", err);
  });
