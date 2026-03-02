const fs = require("fs");
const path = require("path");
const { parse } = require("csv-parse");

export function parseCsvToObjects(filePath) {
    return new Promise((resolve, reject) => {
      const results: any[] = [];
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
        .on("data", (row: any) => results.push(row))
        .on("end", () => resolve(results))
        .on("error", (err) => reject(err));
  
      stream.on("error", (err) => reject(err));
    });
  }