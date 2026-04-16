using System;
using System.Collections.Generic;
using System.Text;

namespace Models
{
    public class ManifestLoadingStatus
    {
        public int PalletNumber { get; set; }
        public int PickedQuantity { get; set; }
        public int PicklistNumber { get; set; }
        public string CatalogCode { get; set; }
        public int PalletUnits { get; set; }
        public string BestBefore { get; set; }
        public int OldPalletNumber { get; set; }
        public int ManifestNumber { get; set; }
    }
}
