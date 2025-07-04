import { CompanyTypes } from "israeli-bank-scrapers";
import { DomainRuleManager } from "./domainRules.js";

jest.mock("../utils/logger.js", () => ({
  createLogger: jest.fn(() => jest.fn()),
}));

jest.mock("../config.js", () => ({
  config: {
    options: {
      security: {
        firewallSettings: "",
      },
    },
  },
}));

describe("domainRules", () => {
  describe("DomainRuleManager", () => {
    it("should load rules from the provided string", () => {
      const ruleManager = new DomainRuleManager([
        `hapoalim ALLOW api.bankhapoalim.co.il`,
      ]);
      const result = ruleManager.getRule(
        "https://api.bankhapoalim.co.il/login",
        CompanyTypes.hapoalim,
      );
      expect(result).toBe("ALLOW");
    });
  });

  describe("DomainRuleManager.getRule", () => {
    const ruleManager = new DomainRuleManager([
      "visaCal ALLOW cal-online.co.il",
      "visaCal BLOCK google.com",
      "visaCal BLOCK facebook.com",
      "visaCal BLOCK fonts.gstatic.com",
    ]);

    it.each([
      ["ALLOW", "https://cal-online.co.il", CompanyTypes.visaCal],
      ["ALLOW", "https://api.cal-online.co.il", CompanyTypes.visaCal],
      ["BLOCK", "https://google.com", CompanyTypes.visaCal],
      ["BLOCK", "https://www.google.com", CompanyTypes.visaCal],
      ["BLOCK", "https://www.facebook.com", CompanyTypes.visaCal],
      ["BLOCK", "https://fonts.gstatic.com", CompanyTypes.visaCal],
      ["ALLOW", "https://other.gstatic.com", CompanyTypes.visaCal], // Not blocked subdomain
      ["ALLOW", "https://google.com", CompanyTypes.hapoalim], // Different company
    ])("should return %s for %s under %s", (expected, url, company) => {
      expect(ruleManager.getRule(url, company)).toBe(expected);
    });
  });

  describe("DomainRuleManager.hasAnyRule", () => {
    it("should return true if company has rules defined", () => {
      const ruleManager = new DomainRuleManager([
        "hapoalim ALLOW api.bankhapoalim.co.il",
        "leumi BLOCK api.leumi.co.il",
        "visaCal ALLOW cal-online.co.il",
      ]);

      expect(ruleManager.hasAnyRule(CompanyTypes.hapoalim)).toBe(true);
      expect(ruleManager.hasAnyRule(CompanyTypes.leumi)).toBe(true);
      expect(ruleManager.hasAnyRule(CompanyTypes.visaCal)).toBe(true);
    });

    it("should return false if company has no rules defined", () => {
      const ruleManager = new DomainRuleManager([
        "hapoalim ALLOW api.bankhapoalim.co.il",
        "leumi BLOCK api.leumi.co.il",
      ]);

      expect(ruleManager.hasAnyRule(CompanyTypes.max)).toBe(false);
      expect(ruleManager.hasAnyRule(CompanyTypes.isracard)).toBe(false);
    });

    it("should return false with empty rules", () => {
      const ruleManager = new DomainRuleManager([]);

      expect(ruleManager.hasAnyRule(CompanyTypes.hapoalim)).toBe(false);
      expect(ruleManager.hasAnyRule(CompanyTypes.leumi)).toBe(false);
    });
  });
});
