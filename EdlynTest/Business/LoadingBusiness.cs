using System;
using System.Collections.Generic;
using System.Text;
using Abstractions.ServiceInterfaces;
using Models;

namespace Business
{
    public class LoadingBusiness
    {
        private readonly ILoadingService _loadingService;

        public LoadingBusiness(ILoadingService loadingService)
        {
            _loadingService = loadingService;
        }

        public TransactionWrapper GetDeliveryDetails(string customerCode, string catalogCode, int assigneeNumber)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            wrapper = _loadingService.GetDeliveryDetails(customerCode, catalogCode, assigneeNumber);
            return wrapper;
        }

        public TransactionWrapper GetPalletsInManifest(int manifestNo)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            List<LoadingPallet> loadingPallets = new List<LoadingPallet>();

            wrapper = _loadingService.GetPalletsInManifest(manifestNo);
            if (wrapper.IsSuccess == false)
            {
                return wrapper;
            }

            loadingPallets = wrapper.ResultSet[0] as List<LoadingPallet>;

            wrapper = _loadingService.GetCarrier(manifestNo);
            if (wrapper.IsSuccess == false)
            {
                return wrapper;
            }

            loadingPallets[0].CarrierName = wrapper.ResultSet[0] as string;
            wrapper.ResultSet[0] = loadingPallets;

            return wrapper;
        }

        public TransactionWrapper GetPicklistNumber(int invoiceNumber)
        {
            TransactionWrapper wrapper = _loadingService.GetPicklistNumber(invoiceNumber);
            return wrapper;
        }

        public TransactionWrapper UpdatePalletDetailDespatched(int palletNumber)
        {
            TransactionWrapper wrapper = _loadingService.UpdatePalletDetailDespatched(palletNumber);
            return wrapper;
        }
    }
}
