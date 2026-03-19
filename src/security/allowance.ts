import { Zero, MaxUint256 } from "@ethersproject/constants";
import { BigNumber } from "@ethersproject/bignumber";
import { parseUnits } from "@ethersproject/units";
import { Wallet } from "@ethersproject/wallet";
import { JsonRpcProvider } from "@ethersproject/providers";
import { Contract } from "@ethersproject/contracts";
import { Chain, AssetType, ClobClient } from "@polymarket/clob-client";
import { getContractConfig } from "@polymarket/clob-client";
import { logger } from "../utils/logger";
import { config } from "../config";

// Minimal USDC ERC20 ABI
const USDC_ABI = [
    "function approve(address spender, uint256 amount) external returns (bool)",
    "function allowance(address owner, address spender) external view returns (uint256)",
];

// Minimal ERC1155 ABI for ConditionalTokens
const CTF_ABI = [
    "function setApprovalForAll(address operator, bool approved) external",
    "function isApprovedForAll(address account, address operator) external view returns (bool)",
];

/**
 * Get RPC provider URL based on chain ID
 */
function getRpcUrlCandidates(chainId: number): string[] {
    const out: string[] = [];

    // Highest priority: explicit override
    if (config.rpcUrl) out.push(config.rpcUrl);

    const rpcToken = config.rpcToken;

    if (chainId === 137) {
        if (rpcToken) out.push(`https://polygon-mainnet.g.alchemy.com/v2/${rpcToken}`);
        // Public fallbacks (ordered)
        out.push(
            "https://polygon-rpc.com",
            "https://rpc.ankr.com/polygon",
            "https://polygon.llamarpc.com",
            "https://rpc-mainnet.matic.quiknode.pro"
        );
        return Array.from(new Set(out));
    }

    if (chainId === 80002) {
        if (rpcToken) out.push(`https://polygon-amoy.g.alchemy.com/v2/${rpcToken}`);
        out.push("https://rpc-amoy.polygon.technology");
        return Array.from(new Set(out));
    }

    throw new Error(`Unsupported chain ID: ${chainId}. Supported: 137 (Polygon), 80002 (Amoy)`);
}

async function providerWithTimeout(
    provider: JsonRpcProvider,
    timeoutMs: number
): Promise<void> {
    await Promise.race([
        provider.getNetwork().then(() => undefined),
        new Promise<void>((_, reject) =>
            setTimeout(() => reject(new Error(`RPC timeout after ${timeoutMs}ms`)), timeoutMs)
        ),
    ]);
}

async function getWorkingProvider(chainId: number): Promise<{ provider: JsonRpcProvider; rpcUrl: string }> {
    const candidates = getRpcUrlCandidates(chainId);
    const errors: string[] = [];
    for (const rpcUrl of candidates) {
        const provider = new JsonRpcProvider(rpcUrl);
        try {
            await providerWithTimeout(provider, 7000);
            return { provider, rpcUrl };
        } catch (e) {
            errors.push(`${rpcUrl} -> ${e instanceof Error ? e.message : String(e)}`);
        }
    }
    throw new Error(
        `Could not connect to any RPC endpoint for chainId=${chainId}. ` +
            `Set RPC_URL in .env. Attempts:\n- ${errors.join("\n- ")}`
    );
}

/**
 * Approve USDC to Polymarket contracts (maximum allowance)
 * Approves USDC for both ConditionalTokens and Exchange contracts
 */
export async function approveUSDCAllowance(): Promise<void> {
    const privateKey = config.requirePrivateKey();

    const chainId = (config.chainId || Chain.POLYGON) as Chain;
    const contractConfig = getContractConfig(chainId);
    
    // Get RPC URL and create provider
    const { provider, rpcUrl } = await getWorkingProvider(chainId);
    const wallet = new Wallet(privateKey, provider);
    
    const address = await wallet.getAddress();
    logger.info(`正在为地址批准 USDC 授权: ${address}, 链ID: ${chainId}`);
    logger.info(`RPC: ${rpcUrl}`);
    logger.info(`USDC 合约: ${contractConfig.collateral}`);
    logger.info(`ConditionalTokens 合约: ${contractConfig.conditionalTokens}`);
    logger.info(`Exchange 合约: ${contractConfig.exchange}`);

    // Create USDC contract instance
    const usdcContract = new Contract(contractConfig.collateral, USDC_ABI, wallet);

    // Configure gas options
    let gasOptions: { gasPrice?: BigNumber; gasLimit?: number } = {};
    try {
        const gasPrice = await provider.getGasPrice();
        gasOptions = {
            gasPrice: gasPrice.mul(120).div(100), // 20% buffer
            gasLimit: 200_000,
        };
    } catch (error) {
        logger.warn("无法获取 gas 价格，使用备用值");
        gasOptions = {
            gasPrice: parseUnits("100", "gwei"),
            gasLimit: 200_000,
        };
    }

    // Check and approve USDC for ConditionalTokens contract
    const ctfAllowance = await usdcContract.allowance(address, contractConfig.conditionalTokens);
    if (!ctfAllowance.eq(MaxUint256)) {
        logger.info(`当前 CTF 授权: ${ctfAllowance.toString()}, 正在设置为 MaxUint256...`);
        const tx = await usdcContract.approve(contractConfig.conditionalTokens, MaxUint256, gasOptions);
        logger.info(`交易哈希: ${tx.hash}`);
        await tx.wait();
        logger.success("✅ USDC 已批准用于 ConditionalTokens 合约");
    } else {
        logger.info("✅ USDC 已批准用于 ConditionalTokens 合约 (MaxUint256)");
    }

    // Check and approve USDC for Exchange contract
    const exchangeAllowance = await usdcContract.allowance(address, contractConfig.exchange);
    if (!exchangeAllowance.eq(MaxUint256)) {
        logger.info(`当前 Exchange 授权: ${exchangeAllowance.toString()}, 正在设置为 MaxUint256...`);
        const tx = await usdcContract.approve(contractConfig.exchange, MaxUint256, gasOptions);
        logger.info(`交易哈希: ${tx.hash}`);
        await tx.wait();
        logger.success("✅ USDC 已批准用于 Exchange 合约");
    } else {
        logger.info("✅ USDC 已批准用于 Exchange 合约 (MaxUint256)");
    }

    // Check and approve ConditionalTokens (ERC1155) for Exchange contract
    const ctfContract = new Contract(contractConfig.conditionalTokens, CTF_ABI, wallet);
    const isApproved = await ctfContract.isApprovedForAll(address, contractConfig.exchange);
    
    if (!isApproved) {
        logger.info("正在批准 ConditionalTokens 用于 Exchange 合约...");
        const tx = await ctfContract.setApprovalForAll(contractConfig.exchange, true, gasOptions);
        logger.info(`交易哈希: ${tx.hash}`);
        await tx.wait();
        logger.success("✅ ConditionalTokens 已批准用于 Exchange 合约");
    } else {
        logger.info("✅ ConditionalTokens 已批准用于 Exchange 合约");
    }

    // If negRisk is enabled, also approve for negRisk contracts
    const negRisk = config.negRisk;
    if (negRisk) {
        // Approve USDC for NegRiskAdapter
        const negRiskAdapterAllowance = await usdcContract.allowance(address, contractConfig.negRiskAdapter);
        if (!negRiskAdapterAllowance.eq(MaxUint256)) {
            logger.info(`当前 NegRiskAdapter 授权: ${negRiskAdapterAllowance.toString()}, 正在设置为 MaxUint256...`);
            const tx = await usdcContract.approve(contractConfig.negRiskAdapter, MaxUint256, gasOptions);
            logger.info(`交易哈希: ${tx.hash}`);
            await tx.wait();
            logger.success("✅ USDC 已批准用于 NegRiskAdapter");
        }

        // Approve USDC for NegRiskExchange
        const negRiskExchangeAllowance = await usdcContract.allowance(address, contractConfig.negRiskExchange);
        if (!negRiskExchangeAllowance.eq(MaxUint256)) {
            logger.info(`当前 NegRiskExchange 授权: ${negRiskExchangeAllowance.toString()}, 正在设置为 MaxUint256...`);
            const tx = await usdcContract.approve(contractConfig.negRiskExchange, MaxUint256, gasOptions);
            logger.info(`交易哈希: ${tx.hash}`);
            await tx.wait();
            logger.success("✅ USDC 已批准用于 NegRiskExchange");
        }

        // Approve ConditionalTokens for NegRiskExchange
        const isNegRiskApproved = await ctfContract.isApprovedForAll(address, contractConfig.negRiskExchange);
        if (!isNegRiskApproved) {
            logger.info("正在批准 ConditionalTokens 用于 NegRiskExchange...");
            const tx = await ctfContract.setApprovalForAll(contractConfig.negRiskExchange, true, gasOptions);
            logger.info(`交易哈希: ${tx.hash}`);
            await tx.wait();
            logger.success("✅ ConditionalTokens 已批准用于 NegRiskExchange");
        }

        // Approve ConditionalTokens for NegRiskAdapter
        const isNegRiskAdapterApproved = await ctfContract.isApprovedForAll(address, contractConfig.negRiskAdapter);
        if (!isNegRiskAdapterApproved) {
            logger.info("正在批准 ConditionalTokens 用于 NegRiskAdapter...");
            const tx = await ctfContract.setApprovalForAll(contractConfig.negRiskAdapter, true, gasOptions);
            logger.info(`交易哈希: ${tx.hash}`);
            await tx.wait();
            logger.success("✅ ConditionalTokens 已批准用于 NegRiskAdapter");
        }
    }

    logger.success("所有授权已成功批准！");
}

/**
 * Update balance allowance in CLOB API after setting on-chain allowances
 * This syncs the on-chain allowance state with the CLOB API
 */
export async function updateClobBalanceAllowance(client: ClobClient): Promise<void> {
    try {
        logger.info("正在更新 CLOB API 的 USDC 余额授权...");
        await client.updateBalanceAllowance({ asset_type: AssetType.COLLATERAL });
        logger.success("✅ CLOB API 的 USDC 余额授权已更新");
    } catch (error) {
        logger.error(`更新 CLOB 余额授权失败: ${error instanceof Error ? error.message : String(error)}`);
        throw error;
    }
}

/**
 * Approve ConditionalTokens for Exchange after buying tokens
 * This ensures tokens are approved immediately after purchase so they can be sold without delay
 * Note: ERC1155 uses setApprovalForAll which approves all tokens at once (including newly bought ones)
 */
export async function approveTokensAfterBuy(): Promise<void> {
    const privateKey = config.requirePrivateKey();

    const chainId = (config.chainId || Chain.POLYGON) as Chain;
    const contractConfig = getContractConfig(chainId);
    
    // Get RPC URL and create provider
    const { provider, rpcUrl } = await getWorkingProvider(chainId);
    const wallet = new Wallet(privateKey, provider);
    
    const address = await wallet.getAddress();
    logger.info(`RPC: ${rpcUrl}`);
    const ctfContract = new Contract(contractConfig.conditionalTokens, CTF_ABI, wallet);

    // Configure gas options
    let gasOptions: { gasPrice?: BigNumber; gasLimit?: number } = {};
    try {
        const gasPrice = await provider.getGasPrice();
        gasOptions = {
            gasPrice: gasPrice.mul(120).div(100), // 20% buffer
            gasLimit: 200_000,
        };
    } catch (error) {
        gasOptions = {
            gasPrice: parseUnits("100", "gwei"),
            gasLimit: 200_000,
        };
    }

    // Check if ConditionalTokens are approved for Exchange
    const isApproved = await ctfContract.isApprovedForAll(address, contractConfig.exchange);
    
    if (!isApproved) {
        logger.info("正在批准 ConditionalTokens 用于 Exchange (买入后)...");
        const tx = await ctfContract.setApprovalForAll(contractConfig.exchange, true, gasOptions);
        logger.info(`交易哈希: ${tx.hash}`);
        await tx.wait();
        logger.success("✅ ConditionalTokens 已批准用于 Exchange");
    }

    // If negRisk is enabled, also check negRisk contracts
    const negRisk = config.negRisk;
    if (negRisk) {
        const isNegRiskApproved = await ctfContract.isApprovedForAll(address, contractConfig.negRiskExchange);
        if (!isNegRiskApproved) {
            logger.info("正在批准 ConditionalTokens 用于 NegRiskExchange (买入后)...");
            const tx = await ctfContract.setApprovalForAll(contractConfig.negRiskExchange, true, gasOptions);
            logger.info(`交易哈希: ${tx.hash}`);
            await tx.wait();
            logger.success("✅ ConditionalTokens 已批准用于 NegRiskExchange");
        }
    }
}

