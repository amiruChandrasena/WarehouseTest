using System;
using System.Collections.Generic;
using System.Text;

namespace Models.Dto
{
    public class PickingDto
    {
        public string ScanData { get; set; }
        public string Originator { get; set; }
        public string WarehouseCode { get; set; }
        public string RoomCode { get; set; }
        public int ManifestNumber { get; set; }
        public int PicklistNumber { get; set; }
        public int PalletNumber { get; set; }
        public string PickingLabel { get; set; }
        public int NoNegativePickBin { get; set; }
        public int PickingFromPickPhase { get; set; }
        public int PickingPartOfPalletNumber { get; set; }
        public int PickingPartOfPallet { get; set; }
        public int PalletQuantity { get; set; }
        public int PickingQuantity { get; set; }
        public string BinLocation { get; set; }
        public string IsTransfer { get; set; }
        public CatalogItem CatalogItem { get; set; }
        public int PalletCount { get; set; } = 0;
        public int PalletSpaces { get; set; } = 0;
        public ManifestLoadingStatus ManifestLoadStatus { get; set; }
        public List<ManifestLoadingStatus> PickedItems { get; set; }
        public List<PicklistItem> PicklistItems { get; set; }
        
    }
}
