using System;
using System.Collections.Generic;
using System.Text;

namespace Models
{
    public class PickerAllocation
    {
        public string CatalogCode { get; set; }
        public string CatalogDescription { get; set; }
        public string CustomerCode { get; set; }
        public string CustomerOrderNumber { get; set; }
        public DateTime DepartureDate { get; set; }
        public DateTime DepartureTime { get; set; }
        public string FeedbackStatus { get; set; }
        public int LoadOption { get; set; }
        public int ManifestNumber { get; set; }
        public int OpenPalletNumber { get; set; }
        public string Originator { get; set; }
        public int ParallelOption { get; set; }
        public int PickedQuantity { get; set; }
        public int PicklistNumber { get; set; }
        public int PicklistNumberOriginal { get; set; }
        public int PicklistSequenceNumber { get; set; }
        public int PicklistSequenceNumberHidden { get; set; }
        public string RegistrationNumber { get; set; }
        public int RequiredQuantity { get; set; }
        public string Run { get; set; }
        public int Select { get; set; }
        public int SequenceNumber { get; set; }
        public string Status { get; set; }
        public string StatusDescription { get; set; }
        public int TandumOption { get; set; }
        public float Tare { get; set; }
    }
}
