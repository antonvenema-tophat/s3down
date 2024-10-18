import { GetObjectCommand, ListObjectsV2Command, S3Client } from "@aws-sdk/client-s3";
import { fromIni } from "@aws-sdk/credential-providers";
import chalk from "chalk";
import fs from "fs";
import path from "path";

const client = new S3Client({
  region: "us-east-1",
  credentials: fromIni({
    profile: "tophat-org-root",
  }),
});

(async () => {
  const listObjectsResponse = await client.send(new ListObjectsV2Command({
    Bucket: "tophat-cost-and-usage-reports",
    Prefix: "cur/DetailedBilling/20240901-20241001/",
  }));
  if (!listObjectsResponse.Contents) {
    console.info(chalk.red("No objects found."));
    return;
  }
  console.info(chalk.blue(`Processing ${listObjectsResponse.KeyCount} objects...`));
  let index = 1;
  for (const object of listObjectsResponse.Contents) {
    if (!object.Key || !object.Size) continue;

    const outputPath = path.join(__dirname, `${object.Key}`);
    try { await fs.promises.mkdir(path.dirname(outputPath), { recursive: true }); } catch { }
    if (fs.existsSync(outputPath)) {
      console.info(chalk.yellow(`${index++}/${listObjectsResponse.KeyCount} [skip] ${object.Key}`));
      continue;
    }

    console.info(chalk.green(`${index++}/${listObjectsResponse.KeyCount} (${(object.Size / 1000000).toLocaleString()} MB) ${object.Key}`));
    const getObjectResponse = await client.send(new GetObjectCommand({
      Bucket: "thm-billing",
      Key: object.Key,
    }));
    if (!getObjectResponse.Body) continue;
    const buffer = await getObjectResponse.Body.transformToByteArray();

    const file = await fs.promises.open(outputPath, "w");
    try {
      await file.write(buffer);
      await file.close();
    } catch (error) {
      console.error(chalk.red(error));
      try { await file.close(); } catch { }
      try { await fs.promises.rm(outputPath); } catch { }
    }
  }
})();