export const FEISHU_PERMISSION_ERROR_CODE = 99991672;

export type FeishuPermissionErrorInfo = {
  code: number;
  message: string;
  grantUrl?: string;
};

function extractGrantUrlFromMessage(msg: string): string | undefined {
  // Feishu often embeds a permission grant URL directly in the error message.
  // Example format contains "... https://open.feishu.cn/app/...".
  const match = msg.match(/https:\/\/[^\s,]+\/app\/[^\s,]+/);
  return match?.[0];
}

export class FeishuApiError extends Error {
  readonly code: number;
  readonly grantUrl?: string;

  constructor(params: { code: number; message: string }) {
    super(params.message);
    this.name = "FeishuApiError";
    this.code = params.code;
    this.grantUrl =
      params.code === FEISHU_PERMISSION_ERROR_CODE
        ? extractGrantUrlFromMessage(params.message)
        : undefined;
  }
}

export function isFeishuApiError(err: unknown): err is FeishuApiError {
  return err instanceof FeishuApiError;
}

/**
 * Best-effort extraction of Feishu permission error info from:
 * - our FeishuApiError wrapper
 * - SDK/HTTP client errors that include `response.data.{code,msg}`
 */
export function extractPermissionError(err: unknown): FeishuPermissionErrorInfo | null {
  if (!err || typeof err !== "object") {
    return null;
  }

  if (err instanceof FeishuApiError && err.code === FEISHU_PERMISSION_ERROR_CODE) {
    return { code: err.code, message: err.message, grantUrl: err.grantUrl };
  }

  // Axios-like error: err.response.data contains the Feishu error.
  const axiosErr = err as { response?: { data?: unknown } };
  const data = axiosErr.response?.data;
  if (!data || typeof data !== "object") {
    return null;
  }

  const feishuErr = data as { code?: number; msg?: string };
  if (feishuErr.code !== FEISHU_PERMISSION_ERROR_CODE) {
    return null;
  }
  const msg = feishuErr.msg ?? "";
  return {
    code: feishuErr.code,
    message: msg,
    grantUrl: extractGrantUrlFromMessage(msg),
  };
}
