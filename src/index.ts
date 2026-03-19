import { logger } from "./utils/logger";
import { createCredential } from "./security/createCredential";
import { approveUSDCAllowance, updateClobBalanceAllowance } from "./security/allowance";
import { getClobClient } from "./providers/clobclient";
import { waitForMinimumUsdcBalance } from "./utils/balance";
import { config } from "./config";

import { CopytradeArbBot } from "./order-builder/copytrade";
import { setupConsoleFileLogging } from "./utils/console-file";

// Capture ALL console output (stdout/stderr) into a local file.
// Configure via env var:
// - LOG_FILE_PATH="logs/bot-{date}.log" (daily) or "logs/bot.log" (single file)
// - LOG_DIR="logs" and LOG_FILE_PREFIX="bot" (daily; used if LOG_FILE_PATH not set)
setupConsoleFileLogging({
    logFilePath: config.logging.logFilePath, // supports "{date}" placeholder
    logDir: config.logging.logDir,
    filePrefix: config.logging.logFilePrefix,
});

function msUntilNext15mBoundary(now: Date = new Date()): number {
    const d = new Date(now);
    d.setSeconds(0, 0);
    const m = d.getMinutes();
    const nextMin = (Math.floor(m / 15) + 1) * 15;
    d.setMinutes(nextMin, 0, 0);
    return Math.max(0, d.getTime() - now.getTime());
}

async function waitForNextMarketStart(): Promise<void> {
    const ms = msUntilNext15mBoundary();
    if (ms <= 0) return;
    logger.info(
        `等待下一个15分钟市场开始: ${Math.ceil(ms / 1000)}秒 (在下一个边界开始)`
    );
    await new Promise((resolve) => setTimeout(resolve, ms));
    logger.success("下一个15分钟市场已开始 — 现在启动机器人");
}

async function waitMs(ms: number, label: string): Promise<void> {
    if (!(ms > 0)) return;
    logger.info(`等待 ${Math.ceil(ms / 1000)}秒 ${label}...`);
    await new Promise((resolve) => setTimeout(resolve, ms));
}

async function main() {
    logger.info("正在启动机器人...");

    // Create credentials if they don't exist
    const credential = await createCredential();
    if (credential) {
        logger.info("凭证已就绪");
    }

    const clobClient = await getClobClient();
    const isSimulation = config.copytrade.simulate;

    // Approve USDC allowances to Polymarket contracts (skip in simulation mode)
    if (clobClient) {
        if (isSimulation) {
            logger.info("🧪 [模拟模式] 跳过 USDC 授权批准和余额检查");
        } else {
            try {
                logger.info("正在批准 USDC 授权到 Polymarket 合约...");
                await approveUSDCAllowance();

                // Update CLOB API to sync with on-chain allowances
                logger.info("正在同步授权到 CLOB API...");
                await updateClobBalanceAllowance(clobClient);
            } catch (error: unknown) {
                const msg = error instanceof Error ? error.message : String(error);
                const code = (error as { code?: string })?.code;
                const isInsufficientFunds =
                    code === "INSUFFICIENT_FUNDS" ||
                    /insufficient funds/i.test(msg);
                if (isInsufficientFunds) {
                    logger.error("资金不足: 您的钱包没有 POL (MATIC) 用于支付 gas 费。");
                    logger.error("请在 Polygon 上为您的钱包添加 POL 以运行此机器人: https://polygonscan.com/address/YOUR_WALLET");
                    logger.warn("继续运行但不授权 - 在您为钱包充值之前订单可能会失败。");
                } else {
                    logger.error("批准 USDC 授权失败", error);
                    logger.warn("继续运行但不授权 - 订单可能会失败");
                }
            }

            // Validation gate: proceed only once available USDC balance is >= $1
            const { ok, available, allowance, balance } = await waitForMinimumUsdcBalance(clobClient, config.bot.minUsdcBalance, {
                pollIntervalMs: 15_000,
                timeoutMs: 0, // wait indefinitely
                logEveryPoll: true,
            });
            logger.info(
                `等待最小 USDC 余额 ==> 通过=${ok} 可用=${available} 授权=${allowance} 余额=${balance}`
            );
            logger.success("钱包已充值");
        }
        // Next step:
        if (config.bot.waitForNextMarketStart) {
            await waitForNextMarketStart();
        } else {
            logger.info("跳过等待下一个15分钟市场开始 (立即从状态恢复)");
        }
        // Delay trading start to allow previous market to become redeemable (~200s) and be redeemed by worker.
        const copytrade = CopytradeArbBot.fromEnv(clobClient);
        copytrade.start();
    } else {
        logger.error("初始化 CLOB 客户端失败 - 无法继续");
        return;
    }
}

main().catch((error) => {
    logger.error("致命错误", error);
    process.exit(1);
});
