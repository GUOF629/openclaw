declare module "mammoth" {
  export type MammothMessage = { type: string; message: string };
  export type ExtractRawTextResult = { value: string; messages?: MammothMessage[] };
  export function extractRawText(input: { buffer: Buffer }): Promise<ExtractRawTextResult>;
  const mammoth: { extractRawText: typeof extractRawText };
  export default mammoth;
}
