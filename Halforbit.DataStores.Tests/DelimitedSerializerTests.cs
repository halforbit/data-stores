using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit;

namespace Halforbit.DataStores.Tests
{
    public class DelimitedSerializerTests
    {
        [Fact, Trait("Type", "Unit"), Trait("Type", "RunOnBuild")]
        public async Task Serialize_Tsv_IEnumerable_Success()
        {
            var delimitedSerializer = new DelimitedSerializer();

            var records = new[]
            {
                new TestRecord(default, default, default, default, default, default, default, default, default, default, default, default),
                new TestRecord(default, default, default, default, default, default, default, default, default, default, default, default)
            };

            var results = await delimitedSerializer.Serialize(records);

            Assert.Equal(2, records.Count());
        }

        [Fact, Trait("Type", "Unit"), Trait("Type", "RunOnBuild")]
        public async Task Deserialize_Tsv_IReadOnlyList_Success()
        {
            var delimitedSerializer = new DelimitedSerializer();

            var records = await delimitedSerializer.Deserialize<IReadOnlyList<TestRecord>>(
                new UTF8Encoding(false).GetBytes(TsvData));

            Assert.Equal(4, records.Count);
        }

        [Fact, Trait("Type", "Unit"), Trait("Type", "RunOnBuild")]
        public async Task Deserialize_Tsv_IEnumerable_Success()
        {
            var delimitedSerializer = new DelimitedSerializer();

            var records = await delimitedSerializer
                .Deserialize<IEnumerable<TestRecord>>(new UTF8Encoding(false).GetBytes(TsvData));
            
            Assert.Equal(4, records.Count());
        }

        public const string TsvData = @"Id	audience	leadCategory	adTemplateId	repeat	startDate	endDate	includeTerms	termsMessage	photo	adName	title
b857d0aba0194d3fae63bbaa09439f4e	All Shoppers	sales	220adf4a-8cbd-496f-a213-72b5182769e3	false	05/01/2020	05/31/2020	true	Stock #TL4060. MSRP $46,195 Includes: $5,000 All Star Discount $7,000 GM Rebates. All Rebates to Dealer, plus TT&L. Some customers may not qualify. See dealer for details. Offer Ends 5/31/2020	https://photo-cuts.purecars.com/cut/trim/ed0806ec-49af-4a4a-a1bd-2c819fca0a42?w=720&h=540&margin_pct=5	MSRP Discount Incentive | 2020 Chevrolet Silverado 1500 WT - $12,000 off MSRP	Get $12,000 off MSRP on a 2020 Chevrolet Silverado 1500 WT for qualified customers. Complete this form by 05/31/20 to redeem this offer!
2657302f4ece4617a44f45090017c23c	All Shoppers	sales	220adf4a-8cbd-496f-a213-72b5182769e3	false	05/01/2020	05/31/2020	true	Stock #TL4328. MSRP $47,155 Includes: $2,450 All Star Discount $4,750 GM Rebates. All Rebates to Dealer, plus TT&L. See dealer for details. Offer Ends 5/31/2020	https://photo-cuts.purecars.com/cut/trim/75c9ba40-2e41-4b0f-83f8-40f18c9771bc?w=720&h=540&margin_pct=5	MSRP Discount Incentive | 2020 Chevrolet Tahoe LT - $7,200 off MSRP	Get $7,200 off MSRP on a 2020 Chevrolet Tahoe LT for qualified customers. Complete this form by 05/31/20 to redeem this offer!
eeb86a789fa0491e8ab4b2ee0210ed82	All Shoppers	sales	220adf4a-8cbd-496f-a213-72b5182769e3	false	05/01/2020	05/31/2020	true	Stock #TL4328. MSRP $47,155 Includes: $2,450 All Star Discount $4,750 GM Rebates. All Rebates to Dealer, plus TT&L. See dealer for details. Offer Ends 5/31/2020	https://photo-cuts.purecars.com/cut/trim/687811c1-b0b9-4ca6-a24c-9d1982a859af?w=720&h=540&margin_pct=5	MSRP Discount Incentive | 2020 Chevrolet Tahoe LS - $7,200 off MSRP	Get $7,200 off MSRP on a 2020 Chevrolet Tahoe LS for qualified customers. Complete this form by 05/31/20 to redeem this offer!
f8f70d49e44d4ac797d792b5b2735bf6	All Shoppers	sales	220adf4a-8cbd-496f-a213-72b5182769e3	false	05/01/2020	05/31/2020	true	Stock #TL4328. MSRP $47,155 Includes: $2,450 All Star Discount $4,750 GM Rebates. All Rebates to Dealer, plus TT&L. See dealer for details. Offer Ends 5/31/2020	https://photo-cuts.purecars.com/cut/trim/2d1e29cf-ac23-4525-905e-cf4cd3b9de01?w=720&h=540&margin_pct=5	MSRP Discount Incentive | 2020 Chevrolet Tahoe Premier - $7,200 off MSRP	Get $7,200 off MSRP on a 2020 Chevrolet Tahoe Premier for qualified customers. Complete this form by 05/31/20 to redeem this offer!
";

        public class TestRecord
        {
            public TestRecord(
                Guid id,
                string audience,
                string leadCategory,
                Guid adTemplateId,
                bool repeat,
                DateTime startDate,
                DateTime endDate,
                bool includeTerms,
                string termsMessage,
                string photo,
                string adName,
                string title)
            {
                Id = id;
                Audience = audience;
                LeadCategory = leadCategory;
                AdTemplateId = adTemplateId;
                Repeat = repeat;
                StartDate = startDate;
                EndDate = endDate;
                IncludeTerms = includeTerms;
                TermsMessage = termsMessage;
                Photo = photo;
                AdName = adName;
                Title = title;
            }

            public Guid Id { get; }
            public string Audience { get; }
            public string LeadCategory { get; }
            public Guid AdTemplateId { get; }
            public bool Repeat { get; }
            public DateTime StartDate { get; }
            public DateTime EndDate { get; }
            public bool IncludeTerms { get; }
            public string TermsMessage { get; }
            public string Photo { get; }
            public string AdName { get; }
            public string Title { get; }
        }
    }
}
