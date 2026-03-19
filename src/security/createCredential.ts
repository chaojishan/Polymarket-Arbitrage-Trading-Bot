import { ApiKeyCreds, ClobClient, Chain } from "@polymarket/clob-client";
import { writeFileSync, existsSync, readFileSync } from "fs";
import { resolve } from "path";
import { Wallet } from "@ethersproject/wallet";
import { logger } from "../utils/logger";
import { config } from "../config";

export async function createCredential(): Promise<ApiKeyCreds | null> {
    const privateKey = config.privateKey;
    if (!privateKey) return (logger.error("未找到 PRIVATE_KEY"), null);

    // Check if credentials already exist
    // const credentialPath = resolve(process.cwd(), "src/data/credential.json");
    // if (existsSync(credentialPath)) {
    //     logger.info("凭证已存在。返回现有凭证。");
    //     return JSON.parse(readFileSync(credentialPath, "utf-8"));
    // }

    try {
        const wallet = new Wallet(privateKey);
        logger.info(`钱包地址 ${wallet.address}`);
        const chainId = (config.chainId || Chain.POLYGON) as Chain;
        const host = config.clobApiUrl;

        // Create temporary ClobClient just for credential creation
        const clobClient = new ClobClient(host, chainId, wallet);
        let credential: ApiKeyCreds;

        try {
            credential = await clobClient.createOrDeriveApiKey();
        } catch (createError: unknown) {
            const msg = createError instanceof Error ? createError.message : String(createError);
            const data = (createError as { response?: { data?: { error?: string } } })?.response?.data?.error;
            const isCouldNotCreate =
                /Could not create api key/i.test(msg) ||
                (typeof data === "string" && /Could not create api key/i.test(data));
            if (isCouldNotCreate) {
                logger.info("创建 API 密钥失败 (钱包可能已有一个), 正在尝试 deriveApiKey...");
                credential = await clobClient.deriveApiKey();
            } else {
                throw createError;
            }
        }

        await saveCredential(credential);
        logger.success("凭证创建成功");
        return credential;
    } catch (error) {
        logger.error("createCredential 错误", error);
        logger.error(
            `创建凭证时出错: ${error instanceof Error ? error.message : String(error)}`
        );
        return null;
    }
}   

export async function saveCredential(credential: ApiKeyCreds) {
    const credentialPath = resolve(process.cwd(), "src/data/credential.json");
    writeFileSync(credentialPath, JSON.stringify(credential, null, 2));
}