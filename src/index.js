const fs = require("fs");
const path = require("path");
const { parse } = require("csv-parse");

/** Nome da tabela no INSERT. Altere conforme seu banco. */
const TABLE_NAME = "GuaranteedIncomePayments";

/**
 * Lê um arquivo CSV e retorna um array de objetos.
 * Cada objeto usa as chaves do cabeçalho do CSV.
 * @param {string} filePath - Caminho do arquivo CSV
 * @returns {Promise<Object[]>}
 */
function parseCsvToObjects(filePath) {
  return new Promise((resolve, reject) => {
    const results = [];
    const absolutePath = path.isAbsolute(filePath)
      ? filePath
      : path.resolve(process.cwd(), filePath);

    const parser = parse({
      columns: true, // usa a primeira linha como cabeçalho
      skip_empty_lines: true,
      trim: true,
      relax_quotes: true,
      bom: true,
    });

    const stream = fs.createReadStream(absolutePath, { encoding: "utf8" });

    stream
      .pipe(parser)
      .on("data", (row) => results.push(row))
      .on("end", () => resolve(results))
      .on("error", (err) => reject(err));

    stream.on("error", (err) => reject(err));
  });
}

/**
 * Escapa um valor para uso em SQL (aspas simples duplicadas).
 * Valor vazio ou string "null" vira NULL.
 */
function escapeSqlValue(value) {
  const s = value == null ? "" : String(value).trim();
  if (s === "" || s.toLowerCase() === "null") return "NULL";
  return "'" + s.replace(/'/g, "''") + "'";
}

/**
 * Gera o SQL INSERT a partir do array de objetos.
 * Colunas e valores seguem exatamente a mesma ordem (chaves do primeiro objeto).
 */
function buildInsertSql(tableName, rows) {
  if (rows.length === 0) return "";

  const columns = Object.keys(rows[0]);
  const columnList = columns.join(", ");

  const valuesLines = rows.map((row) => {
    const values = columns.map((col) => escapeSqlValue(row[col]));
    return `(${values.join(", ")})`;
  });

  return `INSERT INTO ${tableName} (${columnList})\nVALUES\n${valuesLines.join(",\n")}`;
}

async function main() {
  const csvPath = path.join(__dirname, "..", "teste.csv");
  const outputPath = path.join(__dirname, "..", "insert.txt");

  try {
    const dados = await parseCsvToObjects(csvPath);
    console.log(`Total de registros: ${dados.length}`);

    const sql = buildInsertSql(TABLE_NAME, dados);
    fs.writeFileSync(outputPath, sql, "utf8");
    console.log(`Arquivo gerado: ${outputPath}`);
  } catch (err) {
    console.error("Erro ao ler/parsear o CSV ou gravar o arquivo:", err.message);
    process.exit(1);
  }
}

main();
