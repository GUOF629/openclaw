import type * as Lark from "@larksuiteoapi/node-sdk";
import type { OpenClawPluginApi } from "openclaw/plugin-sdk";
import { resolveDefaultFeishuAccountId, resolveFeishuAccount } from "./accounts.js";
import { createFeishuClient } from "./client.js";
import { extractPermissionError } from "./errors.js";
import { FeishuPermSchema, type FeishuPermParams } from "./perm-schema.js";
import { resolveToolsConfig } from "./tools-config.js";

// ============ Helpers ============

function json(data: unknown) {
  return {
    content: [{ type: "text" as const, text: JSON.stringify(data, null, 2) }],
    details: data,
  };
}

type ListTokenType =
  | "doc"
  | "sheet"
  | "file"
  | "wiki"
  | "bitable"
  | "docx"
  | "mindnote"
  | "minutes"
  | "slides";
type CreateTokenType =
  | "doc"
  | "sheet"
  | "file"
  | "wiki"
  | "bitable"
  | "docx"
  | "folder"
  | "mindnote"
  | "minutes"
  | "slides";
type MemberType =
  | "email"
  | "openid"
  | "unionid"
  | "openchat"
  | "opendepartmentid"
  | "userid"
  | "groupid"
  | "wikispaceid";
type PermType = "view" | "edit" | "full_access";

// ============ Actions ============

async function listMembers(client: Lark.Client, token: string, type: string) {
  const res = await client.drive.permissionMember.list({
    path: { token },
    params: { type: type as ListTokenType },
  });
  if (res.code !== 0) {
    throw new Error(res.msg);
  }

  return {
    members:
      res.data?.items?.map((m) => ({
        member_type: m.member_type,
        member_id: m.member_id,
        perm: m.perm,
        name: m.name,
      })) ?? [],
  };
}

async function addMember(
  client: Lark.Client,
  token: string,
  type: string,
  memberType: string,
  memberId: string,
  perm: string,
) {
  const res = await client.drive.permissionMember.create({
    path: { token },
    params: { type: type as CreateTokenType, need_notification: false },
    data: {
      member_type: memberType as MemberType,
      member_id: memberId,
      perm: perm as PermType,
    },
  });
  if (res.code !== 0) {
    throw new Error(res.msg);
  }

  return {
    success: true,
    member: res.data?.member,
  };
}

async function removeMember(
  client: Lark.Client,
  token: string,
  type: string,
  memberType: string,
  memberId: string,
) {
  const res = await client.drive.permissionMember.delete({
    path: { token, member_id: memberId },
    params: { type: type as CreateTokenType, member_type: memberType as MemberType },
  });
  if (res.code !== 0) {
    throw new Error(res.msg);
  }

  return {
    success: true,
  };
}

// ============ Tool Registration ============

export function registerFeishuPermTools(api: OpenClawPluginApi) {
  api.registerTool(
    (ctx) => {
      const cfg = ctx.config;
      if (!cfg) {
        return null;
      }
      const accountId = ctx.agentAccountId ?? resolveDefaultFeishuAccountId(cfg);
      const account = resolveFeishuAccount({ cfg, accountId });
      if (!account.enabled || !account.configured) {
        return null;
      }

      const toolsCfg = resolveToolsConfig(account.config.tools);
      if (!toolsCfg.perm) {
        return null;
      }

      const client = createFeishuClient(account);
      return {
        name: "feishu_perm",
        label: "Feishu Perm",
        description: "Feishu permission management. Actions: list, add, remove",
        parameters: FeishuPermSchema,
        async execute(_toolCallId, params) {
          const p = params as FeishuPermParams;
          try {
            switch (p.action) {
              case "list":
                return json(await listMembers(client, p.token, p.type));
              case "add":
                return json(
                  await addMember(client, p.token, p.type, p.member_type, p.member_id, p.perm),
                );
              case "remove":
                return json(
                  await removeMember(client, p.token, p.type, p.member_type, p.member_id),
                );
              default:
                // eslint-disable-next-line @typescript-eslint/no-explicit-any -- exhaustive check fallback
                return json({ error: `Unknown action: ${(p as any).action}` });
            }
          } catch (err) {
            const permErr = extractPermissionError(err);
            if (permErr) {
              return json({ error: permErr.message, permission: permErr });
            }
            return json({ error: err instanceof Error ? err.message : String(err) });
          }
        },
      };
    },
    { name: "feishu_perm" },
  );
}
