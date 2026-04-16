using System;
using System.Collections.Generic;
using System.Text;

namespace Models
{
    public class PicklistItem
    {
        public int PicklistNumber { get; set; }
        public string Name { get; set; }
        public string Description { get; set; }
        public string CatalogGroup { get; set; }
        public string CatalogCode { get; set; }
        public DateTime NewDateRequired { get; set; }
        public int ManifestNumber { get; set; }
        public DateTime MixPalShedDate { get; set; }
        public int SourceNumber { get; set; }
        public int SequenceNumber { get; set; }
        public int PickedFullQuantity { get; set; }
        public double FullPalletQuantity { get; set; }
        public double LooseQuantity { get; set; }
        public int UnitsPerPallet { get; set; }
        public string MixPalletReady { get; set; }
        public string CreditStatus { get; set; }
        public double RequiredQuantity { get; set; }
        public int Picked { get; set; }
        public double PickingSequenceB { get; set; }
        public double PickingSequenceP { get; set; }
        public string BinLocation { get; set; }
        public int LicensedPalletNumber { get; set; }
    }
}
