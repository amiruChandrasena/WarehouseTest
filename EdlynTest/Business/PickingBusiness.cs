using System;
using System.Collections.Generic;
using System.Text;
using Models;
using Abstractions.ServiceInterfaces;
using System.Transactions;
using System.Linq;
using System.Globalization;
using System.Threading;
using Models.Dto;
using Common;

namespace Business
{
    public class PickingBusiness
    {
        private readonly IPickingService _pickingService;
        private readonly IPalletService _palletService;
        private CultureInfo culture;

        public PickingBusiness(IPickingService pickingService, IPalletService palletService)
        {
            _pickingService = pickingService;
            _palletService = palletService;
            culture = new CultureInfo("en-AU");
        }

        public TransactionWrapper CheckIt(string pickingLabel)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            CheckItModel checkItModel = new CheckItModel();
            string notFinalisedMessage = "";

            int palletNo = _pickingService.GetPickingLabelPallet(pickingLabel);
            if (palletNo == -1)
            {
                wrapper.IsSuccess = false;
                wrapper.Messages.Add("GetPickingLabelPallet : Error");
                return wrapper;
            }
            else if (palletNo == 0)
            {
                wrapper.IsSuccess = false;
                wrapper.Messages.Add("No pallet found under picking label " + pickingLabel);
                return wrapper;
            }

            wrapper = _pickingService.GetCheckManifestAndPicklist(palletNo);
            if (wrapper.Messages.Count > 0)
            {
                notFinalisedMessage = wrapper.Messages[0];
            }
            if (wrapper.IsSuccess == false)
            {
                return wrapper;
            }

            checkItModel.PicklistNumber = Convert.ToInt32(wrapper.ResultSet[0]);
            checkItModel.ManifestNumber = Convert.ToInt32(wrapper.ResultSet[1]);

            wrapper = _pickingService.GetCheckPalletCount(checkItModel.ManifestNumber);
            if (wrapper.IsSuccess == false)
            {
                return wrapper;
            }

            List<int> palletNumbers = wrapper.ResultSet[0] as List<int>;

            int palletCount = 0;
            int count = 0;
            int previousPallet = 0;

            if (palletNumbers.Count > 0)
            {
                for (int i = 0; i < palletNumbers.Count; i++)
                {
                    if (previousPallet != palletNumbers[i])
                    {
                        count += 1;
                        if (palletNumbers[i] == palletNo)
                        {
                            palletCount = count;
                        }
                    }
                    previousPallet = palletNumbers[i];
                }
            }

            checkItModel.PalletCount = palletCount.ToString() + " of " + count.ToString();

            wrapper = _pickingService.GetCheckCarrierCode(checkItModel.ManifestNumber);
            if (wrapper.IsSuccess == false)
            {
                return wrapper;
            }

            checkItModel.CarrierCode = wrapper.ResultSet[0] as string;

            wrapper = _pickingService.GetCheckCustomerAndAddress(checkItModel.PicklistNumber);
            if (wrapper.IsSuccess == false)
            {
                return wrapper;
            }

            checkItModel.CustomerName = wrapper.ResultSet[0] as string;
            checkItModel.Address1 = wrapper.ResultSet[1] as string;
            checkItModel.Address2 = wrapper.ResultSet[2] as string;

            wrapper.ResultSet.Clear();
            if (!String.IsNullOrEmpty(notFinalisedMessage))
            {
                wrapper.Messages.Add(notFinalisedMessage);
            }
            wrapper.ResultSet.Add(checkItModel);
            return wrapper;

        }

        public TransactionWrapper ClosePallet(int palletNo, string originator, int manifestNo, int picklistNo, int palletCount, int palletSpaces)
        {
            TransactionWrapper wrapper = new TransactionWrapper();

            PalletLocationLog palletLocationLog = new PalletLocationLog
            {
                Timestamp = DateTime.Now,
                PalletNo = palletNo,
                NewLocation = "CLOSING",
                MovedBy = originator,
                SyncTime = "",
                ManifestNo = manifestNo,
                Remark = "HH closing for " + picklistNo.ToString(),
            };

            bool isSuccess = _pickingService.InsertPalletLocationLog(palletLocationLog);
            wrapper.IsSuccess = isSuccess;
            if (wrapper.IsSuccess == false)
            {
                wrapper.Messages.Add("ClosePallet : Error");
            }

            if (palletCount > 0 || palletSpaces > 0)
            {
                wrapper = _pickingService.UpdatePlistPalletCountAndSpaces(picklistNo, palletCount, palletSpaces);
                if (wrapper.IsSuccess == false)
                {
                    return wrapper;
                }

                //Added By Irosh 2024/10/04 - Need to Update Pallet Header Count from PalletNo
                wrapper = _pickingService.UpdatePalletHeaderPalletCount(palletNo, palletCount);
                if (wrapper.IsSuccess == false)
                {
                    return wrapper;
                }
            }

            return wrapper;
        }

        public TransactionWrapper ClosePickingScreen(int manifestNo, int picklistNo, string originator)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            wrapper = _pickingService.UpdatePickerAllocationStatus("C", manifestNo, picklistNo, originator);
            return wrapper;
        }

        public TransactionWrapper ConfirmItemsInPicklist(ManifestLoadingStatus manLoadStatus, int palletQty, 
            int picklistNo, int manifestNo, string originator, string binLocation, int palletCount, int palletSpaces)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            string warehouseCode = "";
            string roomCode = "";
            string rackCode = "";

            string[] locationParts = binLocation.Split('.');
            if (locationParts.Count() != 3)
            {
                wrapper.IsSuccess = false;
                wrapper.Messages.Add("manifest : Invalid bin location");
                return wrapper;
            }
            else
            {
                warehouseCode = locationParts[0];
                roomCode = locationParts[1];
                rackCode = locationParts[2];
            }

            using (TransactionScope scope = new TransactionScope())
            {
                try
                {
                    wrapper = _pickingService.InsertManifestLoadingStatus(manLoadStatus, palletQty, picklistNo, manifestNo, originator);
                    if (wrapper.IsSuccess == false)
                    {
                        scope.Dispose();
                        return wrapper;
                    }

                    PalletLocationLog palletLocationLog = new PalletLocationLog
                    {
                        Timestamp = DateTime.Now,
                        PalletNo = manLoadStatus.PalletNumber,
                        NewLocation = "PICKED",
                        MovedBy = originator,
                        SyncTime = "",
                        ManifestNo = manifestNo,
                        Remark = "HH Picked"
                    };

                    wrapper.IsSuccess = _pickingService.InsertPalletLocationLog(palletLocationLog);
                    if (wrapper.IsSuccess == false)
                    {
                        scope.Dispose();
                        wrapper.Messages.Add("InsertPalletLocationLog : Error");
                        return wrapper;
                    }

                    wrapper = _pickingService.UpdatePalletLocationToPicked(manLoadStatus.PalletNumber, "");
                    if (wrapper.IsSuccess == false)
                    {
                        scope.Dispose();
                        return wrapper;
                    }

                    wrapper = _pickingService.SetUsedCellCount(warehouseCode, roomCode, rackCode, manLoadStatus.CatalogCode, "");
                    if (wrapper.IsSuccess == false)
                    {
                        scope.Dispose();
                        return wrapper;
                    }

                    wrapper = _pickingService.UpdatePlistPalletCountAndSpaces(picklistNo, 1, 1);
                    if (wrapper.IsSuccess == false)
                    {
                        return wrapper;
                    }

                    //Added By Irosh 2024/10/04 - Need to Update Pallet Header Count from PalletNo
                    wrapper = _pickingService.UpdatePalletHeaderPalletCount(manLoadStatus.PalletNumber, 1);
                    if (wrapper.IsSuccess == false)
                    {
                        return wrapper;
                    }

                    if (palletCount > 0 || palletSpaces > 0)
                    {
                        wrapper = _pickingService.UpdatePlistPalletCountAndSpaces(picklistNo, palletCount, palletSpaces);
                        if (wrapper.IsSuccess == false)
                        {
                            return wrapper;
                        }

                        //Added By Irosh 2024/10/04 - Need to Update Pallet Header Count from PalletNo
                        wrapper = _pickingService.UpdatePalletHeaderPalletCount(manLoadStatus.PalletNumber, palletCount);
                        if (wrapper.IsSuccess == false)
                        {
                            return wrapper;
                        }
                    }

                    scope.Complete();
                    
                    wrapper.IsSuccess = true;
                }
                catch (Exception e)
                {
                    scope.Dispose();
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add(e.Message);
                    return wrapper;
                }
            }

            return wrapper;
        }

        public TransactionWrapper ConfirmPick(CatalogItem catalogItem, List<ManifestLoadingStatus> pickedItems,
                                                List<PicklistItem> picklistItems, int noNegativePickBin, int pickingFromPickPhase,
                                               int pickedQuantity, int openPalletNo, int pickingPartOfPalletNo, string pickingCatalogCode,
                                               int pickingPartOfPallet, int picklistNumber, int manifestNo, string originator, string binLocation)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            string[] rackParts = binLocation.Split(".");

            wrapper = _pickingService.GetWarehouseRack(rackParts[0], rackParts[1], rackParts[2]);
            if (wrapper.IsSuccess == false)
            {
                return wrapper;
            }

            WarehouseRack rack = wrapper.ResultSet[0] as WarehouseRack;
            wrapper.ResultSet.Clear();

            if (noNegativePickBin == 1)
            {
                if (pickingFromPickPhase == 1)
                {
                    float availablePickQuantity = _pickingService.GetAvailablePickingQuantity(rack.LicensedPalletNo,
                                                                                         rack.ReservedCatalogCode, catalogItem.ShelfLife);
                    if (availablePickQuantity == -1)
                    {
                        wrapper.IsSuccess = false;
                        wrapper.Messages.Add("GetAvailablePickQuantity : Error");
                        return wrapper;
                    }
                    else if (availablePickQuantity < pickedQuantity)
                    {
                        wrapper.IsSuccess = false;
                        wrapper.Messages.Add("Please replenish or count pick bin " + rack.WarehouseCode + "." + rack.RoomCode + "." + rack.RackCode);
                        return wrapper;
                    }
                }
            }

            float originalPickedQuantity = pickedQuantity;
            int indexFound = 0;
            if (openPalletNo != 0)
            {
                int licensedPalletNumber = 0;
                string reservedCatalogCode = "";

                if (pickingPartOfPalletNo != 0 && pickingCatalogCode != "" && pickingPartOfPallet == 1)
                {
                    licensedPalletNumber = pickingPartOfPalletNo;
                    reservedCatalogCode = pickingCatalogCode;
                }
                else
                {
                    licensedPalletNumber = rack.LicensedPalletNo;
                    reservedCatalogCode = rack.ReservedCatalogCode;
                }

                if (licensedPalletNumber == 0)
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("Cannot find licensed pallet for location " + rack.WarehouseCode + "." + rack.RoomCode + "." + rack.RackCode);
                    return wrapper;
                }

                bool isFound = false;
                if (catalogItem.CatalogCode.Trim() == reservedCatalogCode.Trim())
                {
                    for (int i = 0; i < picklistItems.Count; i++)
                    {
                        if (picklistItems[i].PicklistNumber == picklistNumber && picklistItems[i].CatalogCode.Trim() == reservedCatalogCode.Trim()
                            && picklistItems[i].RequiredQuantity - picklistItems[i].Picked > 0)
                        {
                            isFound = true;
                            indexFound = i;
                            break;
                        }
                    }
                }

                if (!isFound)
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("Cannot find product " + reservedCatalogCode + " in picking list");
                    return wrapper;
                }

                if ((picklistItems[indexFound].RequiredQuantity - picklistItems[indexFound].Picked) < pickedQuantity)
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("Picked quantity is greater than required quantity");
                    return wrapper;
                }

                using (TransactionScope scope = new TransactionScope())
                {
                    try
                    {
                        List<PalletDetail> foundPalletDetails;
                        wrapper = _pickingService.GetPalletDetailForPickedItems(licensedPalletNumber, picklistItems[indexFound].CatalogCode);
                        if (wrapper.IsSuccess == false)
                        {
                            return wrapper;
                        }
                        else
                        {
                            foundPalletDetails = wrapper.ResultSet[0] as List<PalletDetail>;
                            for (int i = 0; i < foundPalletDetails.Count; i++)
                            {
                                string bestBeforeString = "";
                                try
                                {
                                    bestBeforeString = foundPalletDetails[i].BestBefore.ToString(DateFormats.ddMMyy);
                                }
                                catch (Exception e)
                                {
                                    wrapper.IsSuccess = false;
                                    wrapper.Messages.Add(e.Message);
                                    return wrapper;
                                }

                                bool foundPallet = false;
                                int foundPalletIndex = 0;

                                for (int j = 0; j < pickedItems.Count(); j++)
                                {
                                    if (pickedItems[j].CatalogCode == picklistItems[indexFound].CatalogCode
                                        && pickedItems[j].BestBefore == foundPalletDetails[i].BestBefore.ToString(DateFormats.ddmmyyyywithouttime, culture)
                                        && pickedItems[j].PalletNumber == openPalletNo)
                                    {
                                        foundPallet = true;
                                        foundPalletIndex = 0;
                                        break;
                                    }
                                }

                                if (foundPallet)
                                {
                                    int pickedQuantityTemp = 0;
                                    if (foundPalletDetails[i].PalletUnits > pickedQuantity)
                                    {
                                        pickedQuantityTemp = pickedQuantity;
                                    }
                                    else
                                    {
                                        pickedQuantityTemp = foundPalletDetails[i].PalletUnits;
                                    }

                                    pickedItems[foundPalletIndex].PalletUnits += pickedQuantityTemp;

                                    float updateManifestPalletQty = (float)pickedQuantityTemp / (float)picklistItems[indexFound].UnitsPerPallet;
                                    wrapper = _pickingService.UpdateManifestLoadingStatus(updateManifestPalletQty, pickedQuantityTemp, manifestNo,
                                                                                          picklistItems[indexFound].PicklistNumber, openPalletNo,
                                                                                          reservedCatalogCode, originator, bestBeforeString);
                                    if (wrapper.IsSuccess == false)
                                    {
                                        scope.Dispose();
                                        return wrapper;
                                    }

                                    wrapper = _pickingService.UpdatePalletDetail(pickedQuantityTemp, (float)picklistItems[indexFound].UnitsPerPallet,
                                                                                 licensedPalletNumber, reservedCatalogCode, bestBeforeString);
                                    if (wrapper.IsSuccess == false)
                                    {
                                        scope.Dispose();
                                        return wrapper;
                                    }

                                    wrapper = _pickingService.UpdatePalletDetail(-1 * pickedQuantityTemp, (float)picklistItems[indexFound].UnitsPerPallet,
                                                                                 openPalletNo, reservedCatalogCode, bestBeforeString);
                                    if (wrapper.IsSuccess == false)
                                    {
                                        scope.Dispose();
                                        return wrapper;
                                    }

                                    int rowsAffected = Convert.ToInt32(wrapper.ResultSet[0]);
                                    if (rowsAffected == 0)
                                    {
                                        wrapper = _pickingService.InsertPalletDetail(openPalletNo, rack.WarehouseCode, reservedCatalogCode, pickedQuantityTemp,
                                                                                     bestBeforeString, licensedPalletNumber);
                                        if (wrapper.IsSuccess == false)
                                        {
                                            scope.Dispose();
                                            return wrapper;
                                        }
                                    }
                                }
                                else
                                {
                                    int pickedQtyTemp = 0;
                                    if (foundPalletDetails[i].PalletUnits >= pickedQuantity)
                                    {
                                        pickedQtyTemp = pickedQuantity;
                                    }
                                    else
                                    {
                                        pickedQtyTemp = foundPalletDetails[i].PalletUnits;
                                    }
                                    ManifestLoadingStatus manLoadStatus = new ManifestLoadingStatus
                                    {
                                        PalletNumber = openPalletNo,
                                        PicklistNumber = picklistItems[indexFound].PicklistNumber,
                                        CatalogCode = reservedCatalogCode,
                                        PalletUnits = pickedQtyTemp,
                                        BestBefore = bestBeforeString,
                                        OldPalletNumber = licensedPalletNumber
                                    };


                                    wrapper = _pickingService.InsertManifestLoadingStatus(manLoadStatus, (pickedQtyTemp / picklistItems[indexFound].UnitsPerPallet),
                                                                                          picklistItems[indexFound].PicklistNumber, manifestNo, originator);
                                    if (wrapper.IsSuccess == false)
                                    {
                                        scope.Dispose();
                                        return wrapper;
                                    }

                                    pickedItems.Insert(0, manLoadStatus);

                                    wrapper = _pickingService.UpdatePalletDetail(pickedQtyTemp, picklistItems[indexFound].UnitsPerPallet, licensedPalletNumber,
                                                                                 reservedCatalogCode, bestBeforeString);
                                    if (wrapper.IsSuccess == false)
                                    {
                                        scope.Dispose();
                                        return wrapper;
                                    }

                                    wrapper = _pickingService.InsertPalletDetail(openPalletNo, rack.WarehouseCode, reservedCatalogCode, pickedQtyTemp,
                                                                                 bestBeforeString, licensedPalletNumber);
                                    if (wrapper.IsSuccess == false)
                                    {
                                        scope.Dispose();
                                        return wrapper;
                                    }

                                    pickedQuantity -= pickedQtyTemp;
                                    picklistItems[indexFound].Picked += pickedQtyTemp;
                                }

                                if (pickedQuantity == 0)
                                {
                                    break;
                                }
                            }
                        }

                        PalletLocationLog palletLocationLog = new PalletLocationLog
                        {
                            Timestamp = DateTime.Now,
                            PalletNo = openPalletNo,
                            NewLocation = "PICKING",
                            MovedBy = originator,
                            SyncTime = "",
                            ManifestNo = manifestNo,
                            Remark = (originalPickedQuantity - pickedQuantity).ToString() + " " + rack.WarehouseCode + "." + rack.RoomCode + "." + rack.RackCode + " (" + licensedPalletNumber.ToString() + ")"
                        };

                        wrapper.IsSuccess = _pickingService.InsertPalletLocationLog(palletLocationLog);
                        if (wrapper.IsSuccess == false)
                        {
                            scope.Dispose();
                            wrapper.Messages.Add("InsertPalletLocationLog : Error");
                            return wrapper;
                        }

                        float unitsLeft = originalPickedQuantity - pickedQuantity;
                        wrapper = _pickingService.UpdateWarehouseRoomConfig(unitsLeft, rack.WarehouseCode, rack.RoomCode, rack.RackCode);
                        if (wrapper.IsSuccess == false)
                        {
                            scope.Dispose();
                            return wrapper;
                        }

                        if (pickedQuantity > 0)
                        {
                            string bestBefore = "";
                            int palletUnits = pickedQuantity;
                            int foundPalletIndex = 0;
                            bool isPalletFound = false;

                            for (int i = 0; i < pickedItems.Count(); i++)
                            {
                                if (pickedItems[i].PicklistNumber == picklistItems[indexFound].PicklistNumber
                                    && pickedItems[i].CatalogCode == picklistItems[indexFound].CatalogCode
                                    && pickedItems[i].PalletNumber == openPalletNo)
                                {
                                    foundPalletIndex = i;
                                    isPalletFound = true;
                                    break;
                                }
                            }

                            int pickedQtyTemp = 0;
                            if (isPalletFound)
                            {
                                if (palletUnits >= pickedQuantity)
                                {
                                    pickedQtyTemp = pickedQuantity;
                                }
                                else
                                {
                                    pickedQtyTemp = palletUnits;
                                }

                                pickedItems[foundPalletIndex].PalletUnits += pickedQtyTemp;

                                wrapper = _pickingService.UpdateManifestLoadingStatus(((float)pickedQtyTemp / (float)picklistItems[indexFound].UnitsPerPallet),
                                                                                      pickedQtyTemp, manifestNo, picklistItems[indexFound].PicklistNumber, openPalletNo,
                                                                                      reservedCatalogCode, originator, bestBefore);
                                if (wrapper.IsSuccess == false)
                                {
                                    scope.Dispose();
                                    return wrapper;
                                }

                                wrapper = _pickingService.UpdatePalletDetail(-1 * pickedQtyTemp, (float)picklistItems[indexFound].UnitsPerPallet,
                                                                             licensedPalletNumber, reservedCatalogCode, bestBefore);
                                if (wrapper.IsSuccess == false)
                                {
                                    scope.Dispose();
                                    return wrapper;
                                }

                                int rowsAffected = Convert.ToInt32(wrapper.ResultSet[0]);
                                if (rowsAffected == 0)
                                {
                                    wrapper = _pickingService.InsertPalletDetail(licensedPalletNumber, rack.WarehouseCode, reservedCatalogCode,
                                                                                 pickedQtyTemp * -1, bestBefore, 0);
                                    if (wrapper.IsSuccess == false)
                                    {
                                        scope.Dispose();
                                        return wrapper;
                                    }
                                }

                                wrapper = _pickingService.UpdatePalletDetail(pickedQtyTemp, (float)picklistItems[indexFound].UnitsPerPallet,
                                                                             openPalletNo, reservedCatalogCode, bestBefore);
                                if (wrapper.IsSuccess == false)
                                {
                                    scope.Dispose();
                                    return wrapper;
                                }

                                rowsAffected = Convert.ToInt32(wrapper.ResultSet[0]);
                                if (rowsAffected == 0)
                                {
                                    wrapper = _pickingService.InsertPalletDetail(openPalletNo, rack.WarehouseCode, reservedCatalogCode, pickedQtyTemp,
                                                                                 bestBefore, licensedPalletNumber);
                                    if (wrapper.IsSuccess == false)
                                    {
                                        scope.Dispose();
                                        return wrapper;
                                    }
                                }

                                pickedQuantity -= pickedQtyTemp;
                                picklistItems[indexFound].Picked += pickedQtyTemp;
                            }
                            else
                            {
                                if (palletUnits >= pickedQuantity)
                                {
                                    pickedQtyTemp = pickedQuantity;
                                }
                                else
                                {
                                    pickedQtyTemp = palletUnits;
                                }
                                ManifestLoadingStatus manLoadStatus = new ManifestLoadingStatus
                                {
                                    PalletNumber = openPalletNo,
                                    PicklistNumber = picklistItems[indexFound].PicklistNumber,
                                    CatalogCode = reservedCatalogCode,
                                    PalletUnits = pickedQtyTemp,
                                    BestBefore = foundPalletDetails[foundPalletDetails.Count - 1].BestBefore.ToString(DateFormats.ddMMyy),
                                    OldPalletNumber = licensedPalletNumber
                                };

                                pickedItems.Insert(0, manLoadStatus);

                                wrapper = _pickingService.InsertManifestLoadingStatus(manLoadStatus, (pickedQtyTemp / picklistItems[indexFound].UnitsPerPallet),
                                                                                      picklistItems[indexFound].PicklistNumber, manifestNo,
                                                                                      originator);
                                if (wrapper.IsSuccess == false)
                                {
                                    scope.Dispose();
                                    return wrapper;
                                }

                                wrapper = _pickingService.UpdatePalletDetail(-1 * pickedQtyTemp, picklistItems[indexFound].UnitsPerPallet, licensedPalletNumber,
                                                                             reservedCatalogCode, bestBefore);
                                if (wrapper.IsSuccess == false)
                                {
                                    scope.Dispose();
                                    return wrapper;
                                }

                                int rowsAffected = Convert.ToInt32(wrapper.ResultSet[0]);
                                if (rowsAffected == 0)
                                {
                                    wrapper = _pickingService.InsertPalletDetail(licensedPalletNumber, rack.WarehouseCode, reservedCatalogCode,
                                                                                 -1 * pickedQtyTemp, bestBefore, 0);
                                    if (wrapper.IsSuccess == false)
                                    {
                                        scope.Dispose();
                                        return wrapper;
                                    }
                                }

                                wrapper = _pickingService.InsertPalletDetail(openPalletNo, rack.WarehouseCode, reservedCatalogCode, pickedQtyTemp,
                                                                             bestBefore, licensedPalletNumber);
                                if (wrapper.IsSuccess == false)
                                {
                                    scope.Dispose();
                                    return wrapper;
                                }
                            }

                            palletLocationLog = new PalletLocationLog
                            {
                                Timestamp = DateTime.Now,
                                PalletNo = openPalletNo,
                                NewLocation = "PICKING",
                                MovedBy = originator,
                                SyncTime = "",
                                ManifestNo = manifestNo,
                                Remark = pickedQtyTemp.ToString() + " " + rack.WarehouseCode + "." + rack.RoomCode + "." + rack.RackCode + " (" + licensedPalletNumber.ToString() + ")"
                            };

                            wrapper.IsSuccess = _pickingService.InsertPalletLocationLog(palletLocationLog);
                            if (wrapper.IsSuccess == false)
                            {
                                scope.Dispose();
                                wrapper.Messages.Add("InsertPalletLocationLog : Error");
                                return wrapper;
                            }

                            if (pickingPartOfPalletNo != 0 && !String.IsNullOrEmpty(pickingCatalogCode) && pickingPartOfPallet == 1)
                            {
                                unitsLeft = _pickingService.GetUnitsLeftOnPallet(pickingPartOfPalletNo);

                                if (unitsLeft == -1)
                                {
                                    scope.Dispose();
                                    wrapper.IsSuccess = false;
                                    wrapper.Messages.Add("GetUnitsLeftOnPallet : Error");
                                    return wrapper;
                                }
                                else if (unitsLeft == 0)
                                {
                                    string warehouseCode = "";
                                    string roomCode = "";
                                    string rackCode = "";
                                    binLocation = _pickingService.GetBinLocationOfPallet(pickingPartOfPalletNo);

                                    if (String.IsNullOrEmpty(binLocation))
                                    {
                                        wrapper.IsSuccess = false;
                                        wrapper.Messages.Add("GetBinLocationOfPallet : Error");
                                        return wrapper;
                                    }
                                    else if (binLocation.Contains('.'))
                                    {
                                        string[] binLocationParts = binLocation.Split('.');
                                        if (binLocationParts.Count() != 3)
                                        {
                                            wrapper.IsSuccess = false;
                                            wrapper.Messages.Add("Invalid bin location on pallet " + pickingPartOfPalletNo.ToString());
                                            return wrapper;
                                        }
                                        warehouseCode = binLocationParts[0];
                                        roomCode = binLocationParts[1];
                                        rackCode = binLocationParts[2];

                                        wrapper = _pickingService.UpdatePalletLocationToPicked(pickingPartOfPalletNo, "D");
                                        if (wrapper.IsSuccess == false)
                                        {
                                            scope.Dispose();
                                            return wrapper;
                                        }

                                        wrapper = _pickingService.SetUsedCellCount(warehouseCode, roomCode, rackCode, foundPalletDetails[0].CatalogCode, bestBefore);
                                        if (wrapper.IsSuccess == false)
                                        {
                                            scope.Dispose();
                                            return wrapper;
                                        }
                                    }
                                }
                            }
                            else
                            {
                                wrapper = _pickingService.UpdateWarehouseRoomConfig(pickedQtyTemp, rack.WarehouseCode, rack.RoomCode, rack.RackCode);
                                if (wrapper.IsSuccess == false)
                                {
                                    scope.Dispose();
                                    return wrapper;
                                }
                            }
                        }

                        if (picklistItems[indexFound].RequiredQuantity - picklistItems[indexFound].Picked == 0)
                        {
                            picklistItems.RemoveAt(indexFound);
                        }

                        scope.Complete();
                        wrapper.ResultSet.Add(picklistItems);
                        wrapper.ResultSet.Add(pickedItems);
                        wrapper.IsSuccess = true;
                    }
                    catch (Exception e)
                    {
                        scope.Dispose();
                        wrapper.IsSuccess = false;
                        wrapper.Messages.Add(e.Message);
                        return wrapper;
                    }
                }
            }
            else
            {
                wrapper.IsSuccess = false;
                wrapper.Messages.Add("Please open a new pallet");
                return wrapper;
            }

            return wrapper;
        }

        public TransactionWrapper GetPickingItems(string originator, string warehouseCode, string roomCode, bool isTransfer)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            try
            {
                PickingHeader pickingHeader = new PickingHeader();
                List<string> messages = new List<string>();

                // get negative pick bin 

                pickingHeader.NegativePickBin = _pickingService.GetNegativePickBin();
                if (pickingHeader.NegativePickBin < 0)
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("GetNegativePickBin : Error");
                    return wrapper;
                }

                // get picker allocation details
                if (!isTransfer)
                {
                    wrapper = _pickingService.GetPickerManifestPicklistNumber(originator);
                }
                else
                {
                    wrapper = _pickingService.GetPickerTransferNumber(originator);
                }

                if (wrapper.IsSuccess == false)
                {
                    return wrapper;
                }

                PickerAllocation pickerAllocation = wrapper.ResultSet[0] as PickerAllocation;
                pickingHeader.PicklistNumber = pickerAllocation.PicklistNumber;
                pickingHeader.OpenPalletNumber = pickerAllocation.OpenPalletNumber;

                // get manifest details 
                wrapper = _pickingService.GetManifest(pickerAllocation.ManifestNumber);
                if (wrapper.IsSuccess == false)
                {
                    return wrapper;
                }

                Manifest manifest = wrapper.ResultSet[0] as Manifest;
                pickingHeader.Manifest = manifest;

                // get carrier details
                wrapper = _pickingService.GetCarrier(manifest.CarrierCode);
                if (wrapper.IsSuccess == true)
                {
                    Carrier carrier = wrapper.ResultSet[0] as Carrier;
                    pickingHeader.Carrier = carrier;
                }
                else
                {
                    pickingHeader.Carrier = new Carrier();
                }

                // get picker notes
                wrapper = _pickingService.GetPickerNotes(pickingHeader.Manifest.ManifestNumber);
                if (wrapper.IsSuccess == false)
                {
                    return wrapper;
                }

                pickingHeader.PickingNotes = wrapper.ResultSet[0] as List<PickerNote>;

                // get picklist items
                wrapper = _pickingService.GetPicklistItems(warehouseCode, manifest.ManifestNumber, pickerAllocation.PicklistNumber);
                if (wrapper.IsSuccess == false)
                {
                    return wrapper;
                }

                // check for units per pallet = 0
                using (TransactionScope scope = new TransactionScope())
                {
                    try
                    {
                        for (int i = 0; i < wrapper.ResultSet.Count; i++)
                        {
                            PicklistItem picklistItem = wrapper.ResultSet[i] as PicklistItem;
                            pickingHeader.PicklistItems.Add(picklistItem);
                            if (picklistItem.UnitsPerPallet == 0)
                            {
                                wrapper.Messages.Add("Units Per Pallet is ZERO for " + picklistItem.CatalogCode + ". Please check with office.");
                                PalletLocationLog palletLog = new PalletLocationLog
                                {
                                    Timestamp = DateTime.Now,
                                    PalletNo = 0,
                                    NewLocation = "",
                                    MovedBy = originator,
                                    SyncTime = "",
                                    ManifestNo = manifest.ManifestNumber,
                                    Remark = picklistItem.CatalogCode + " units_per_pallet=0"
                                };

                                bool isSuccess = _pickingService.InsertPalletLocationLog(palletLog);
                                if (!isSuccess)
                                {
                                    scope.Dispose();
                                    wrapper.ResultSet.Clear();
                                    wrapper.IsSuccess = false;
                                    wrapper.Messages.Add("InsertPalletLocationLog1 : Error");
                                    return wrapper;
                                }
                            }
                        }

                        pickingHeader.CustomerName = pickingHeader.PicklistItems[0].Name;

                        // check if another gun is already picking this order
                        int count = _pickingService.GetPickerAllocationCount(manifest.ManifestNumber, pickerAllocation.PicklistNumber);
                        if (count == -1)
                        {
                            wrapper.ResultSet.Clear();
                            wrapper.IsSuccess = false;
                            wrapper.Messages.Add("GetPickerAllocationCount : Error");
                            return wrapper;
                        }
                        else if (count > 0)
                        {
                            PalletLocationLog palletLog = new PalletLocationLog
                            {
                                Timestamp = DateTime.Now,
                                PalletNo = 0,
                                NewLocation = "",
                                MovedBy = originator,
                                SyncTime = "",
                                ManifestNo = manifest.ManifestNumber,
                                Remark = pickerAllocation.PicklistNumber + " marked as PICKING"
                            };

                            messages.Add("Order is marked as PICKING. Please check with office.");
                            bool isSuccess = _pickingService.InsertPalletLocationLog(palletLog);
                            if (!isSuccess)
                            {
                                scope.Dispose();
                                wrapper.ResultSet.Clear();
                                wrapper.IsSuccess = false;
                                wrapper.Messages.Add("InsertPalletLocationLog2 : Error");
                                return wrapper;
                            }
                        }

                        scope.Complete();
                    }
                    catch (Exception e)
                    {
                        scope.Dispose();
                        wrapper.IsSuccess = false;
                        wrapper.Messages.Add(e.Message);
                        return wrapper;
                    }
                }

                // check manifest loading status
                wrapper = _pickingService.GetManifestLoadingStatus(manifest.ManifestNumber, pickerAllocation.PicklistNumber, originator);
                if (wrapper.IsSuccess == false)
                {
                    return wrapper;
                }
                List<ManifestLoadingStatus> manifestLoadingStatuses = new List<ManifestLoadingStatus>();
                if (wrapper.ResultSet.Count > 0)
                {
                    manifestLoadingStatuses = wrapper.ResultSet[0] as List<ManifestLoadingStatus>;
                }

                wrapper.ResultSet.Clear();

                // calculate what has already been picked 
                for (int i = 0; i < manifestLoadingStatuses.Count; i++)
                {
                    for (int j = 0; j < pickingHeader.PicklistItems.Count; j++)
                    {
                        if (pickingHeader.PicklistItems[j].PicklistNumber == manifestLoadingStatuses[i].PicklistNumber
                            && pickingHeader.PicklistItems[j].CatalogCode.Trim() == manifestLoadingStatuses[i].CatalogCode.Trim())
                        {
                            // picked less than required
                            if (pickingHeader.PicklistItems[j].RequiredQuantity - pickingHeader.PicklistItems[j].Picked
                                > manifestLoadingStatuses[i].PalletUnits)
                            {
                                pickingHeader.PicklistItems[j].Picked += manifestLoadingStatuses[i].PalletUnits;
                                pickingHeader.PicklistItems[j].FullPalletQuantity = (pickingHeader.PicklistItems[j].RequiredQuantity - pickingHeader.PicklistItems[j].Picked) / pickingHeader.PicklistItems[j].UnitsPerPallet;
                                pickingHeader.PicklistItems[j].LooseQuantity = (pickingHeader.PicklistItems[j].RequiredQuantity - pickingHeader.PicklistItems[j].Picked) - (Convert.ToInt32(Math.Floor(pickingHeader.PicklistItems[j].FullPalletQuantity)) * pickingHeader.PicklistItems[j].UnitsPerPallet);
                                pickingHeader.PickedItems.Add(manifestLoadingStatuses[i]);
                                break;
                            }
                            // picked same amount as required
                            else if (pickingHeader.PicklistItems[j].RequiredQuantity - pickingHeader.PicklistItems[j].Picked
                                    == manifestLoadingStatuses[i].PalletUnits)
                            {
                                pickingHeader.PickedItems.Add(manifestLoadingStatuses[i]);
                                pickingHeader.PicklistItems.Remove(pickingHeader.PicklistItems[j]);
                                break;
                            }
                            // picked more than required
                            else if (pickingHeader.PicklistItems[j].RequiredQuantity - pickingHeader.PicklistItems[j].Picked
                                    < manifestLoadingStatuses[i].PalletUnits)
                            {
                                messages.Add("Picklist " + pickerAllocation.PicklistNumber + ": Have picked more than required for product " + pickingHeader.PicklistItems[j].CatalogCode);
                                break;
                            }
                            else if (j == pickingHeader.PicklistItems.Count - 1)
                            {
                                messages.Add("Picklist " + pickerAllocation.PicklistNumber + ": Product " + pickingHeader.PicklistItems[j].CatalogCode + " on pallet " + manifestLoadingStatuses[i].PalletNumber + " not found in order.");
                                break;
                            }
                        }
                    }
                }
                /* THIS WAS ALL COMMENTED OUT IN FAVOUR OF A METHOD THAT USED A SINGLE CONNECTION
                 * KEPT AS REFERENCE 

                // sort for picking
                foreach (PicklistItem picklistItem in pickingHeader.PicklistItems)
                {
                    picklistItem.PickingSequenceP = 0;
                    picklistItem.BinLocation = "";

                    if (picklistItem.LooseQuantity > 0)
                    {
                        // get picking sequence for part pallets
                        picklistItem.PickingSequenceP = 999999;
                        wrapper = _pickingService.GetPickingSequenceDetailsPart(picklistItem.CatalogCode, warehouseCode);
                        if (wrapper.IsSuccess == false)
                        {
                            return wrapper;
                        }
                        if (wrapper.ResultSet.Count > 0)
                        {
                            PicklistItem pickSeq = wrapper.ResultSet[0] as PicklistItem;
                            picklistItem.PickingSequenceP = pickSeq.PickingSequenceP;
                            picklistItem.BinLocation = pickSeq.BinLocation;
                            picklistItem.LicensedPalletNumber = pickSeq.LicensedPalletNumber;
                        }
                    }

                    if (picklistItem.FullPalletQuantity > 0 && picklistItem.BinLocation == "")
                    {
                        wrapper = _pickingService.GetPickingSequenceDetailsFull(picklistItem.CatalogCode, warehouseCode, roomCode);
                        if (wrapper.IsSuccess == false)
                        {
                            return wrapper;
                        }
                        if (wrapper.ResultSet.Count > 0)
                        {
                            PicklistItem pickSeq = wrapper.ResultSet[0] as PicklistItem;
                            picklistItem.PickingSequenceB = pickSeq.PickingSequenceB;
                            picklistItem.PickingSequenceP = pickSeq.PickingSequenceP;
                            picklistItem.BinLocation = pickSeq.BinLocation;
                        }
                    }

                    if (picklistItem.BinLocation == "")
                    {
                        wrapper = _pickingService.GetPickingSequenceDetailsNone(picklistItem.CatalogCode, warehouseCode);
                        if (wrapper.IsSuccess == false)
                        {
                            return wrapper;
                        }
                        if (wrapper.ResultSet.Count > 0)
                        {
                            PicklistItem pickSeq = wrapper.ResultSet[0] as PicklistItem;
                            picklistItem.PickingSequenceB = 9999999;
                            picklistItem.PickingSequenceP = 9999999;
                            picklistItem.BinLocation = pickSeq.BinLocation;
                        }
                    }
                }*/

                wrapper = _pickingService.GetPickingSequenceDetails(pickingHeader.PicklistItems, warehouseCode, roomCode);
                if (wrapper.IsSuccess == false)
                {
                    return wrapper;
                }

                pickingHeader.PicklistItems = wrapper.ResultSet[0] as List<PicklistItem>;
                ////pickingHeader.PicklistItems = pickingHeader.PicklistItems.OrderByDescending(s => s.BinLocation).ThenBy(s => s.PickingSequenceP).ThenBy(s => s.PickingSequenceB).ToList();
                //pickingHeader.PicklistItems = pickingHeader.PicklistItems.OrderBy(s => string.IsNullOrEmpty(s.BinLocation)).ThenBy(s => s.BinLocation).ThenBy(s => s.PickingSequenceP).ThenBy(s => s.PickingSequenceB).ToList();

                var picklistItems = pickingHeader.PicklistItems ?? new List<PicklistItem>();

                // 1. Items WITH BinLocation
                var withBinLocation = picklistItems
                    .Where(s => !string.IsNullOrWhiteSpace(s.BinLocation))
                    //.OrderBy(s => s.BinLocation)
                    .OrderBy(s => s.PickingSequenceP)
                    .ThenBy(s => s.PickingSequenceB)
                    .ToList();

                // 2. Items WITHOUT BinLocation
                var withoutBinLocation = picklistItems
                    .Where(s => string.IsNullOrWhiteSpace(s.BinLocation))
                    .OrderBy(s => s.PickingSequenceP)
                    .ThenBy(s => s.PickingSequenceB)
                    .ToList();

                // 3. Combine (WithBin first, then WithoutBin)
                pickingHeader.PicklistItems = withBinLocation.Concat(withoutBinLocation).ToList();


                wrapper = _pickingService.UpdatePickerAllocationStatus("W", pickingHeader.Manifest.ManifestNumber, pickingHeader.PicklistNumber, originator);
                if (wrapper.IsSuccess == false)
                {
                    messages.Add("Could not update picker allocation status");
                }

                wrapper.IsSuccess = true;
                wrapper.ResultSet.Clear();
                wrapper.ResultSet.Add(pickingHeader);
                wrapper.Messages = messages;
                return wrapper;
            }
            catch (Exception e)
            {
                wrapper.IsSuccess = false;
                wrapper.Messages.Add(e.Message);
                return wrapper;
            }
        }

        public TransactionWrapper GetCurrentPicklist(string originator, int picklistNo, int manifestNo, string warehouseCode, string roomCode)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            PickingHeader pickingHeader = new PickingHeader();

            // get picklist items
            wrapper = _pickingService.GetPicklistItems(warehouseCode, manifestNo, picklistNo);
            if (wrapper.IsSuccess == false)
            {
                return wrapper;
            }

            for (int i = 0; i < wrapper.ResultSet.Count; i++)
            {
                PicklistItem picklistItem = wrapper.ResultSet[i] as PicklistItem;
                pickingHeader.PicklistItems.Add(picklistItem);
            }

            // check manifest loading status
            wrapper = _pickingService.GetManifestLoadingStatus(manifestNo, picklistNo, originator);
            if (wrapper.IsSuccess == false)
            {
                return wrapper;
            }
            List<ManifestLoadingStatus> manifestLoadingStatuses = new List<ManifestLoadingStatus>();
            if (wrapper.ResultSet.Count > 0)
            {
                manifestLoadingStatuses = wrapper.ResultSet[0] as List<ManifestLoadingStatus>;
            }

            wrapper.ResultSet.Clear();

            // calculate what has already been picked 
            for (int i = 0; i < manifestLoadingStatuses.Count; i++)
            {
                for (int j = 0; j < pickingHeader.PicklistItems.Count; j++)
                {
                    if (pickingHeader.PicklistItems[j].PicklistNumber == manifestLoadingStatuses[i].PicklistNumber
                        && pickingHeader.PicklistItems[j].CatalogCode.Trim() == manifestLoadingStatuses[i].CatalogCode.Trim())
                    {
                        // picked less than required
                        if (pickingHeader.PicklistItems[j].RequiredQuantity - pickingHeader.PicklistItems[j].Picked
                            > manifestLoadingStatuses[i].PalletUnits)
                        {
                            pickingHeader.PicklistItems[j].Picked += manifestLoadingStatuses[i].PalletUnits;
                            pickingHeader.PicklistItems[j].FullPalletQuantity = (pickingHeader.PicklistItems[j].RequiredQuantity - pickingHeader.PicklistItems[j].Picked) / pickingHeader.PicklistItems[j].UnitsPerPallet;
                            pickingHeader.PicklistItems[j].LooseQuantity = (pickingHeader.PicklistItems[j].RequiredQuantity - pickingHeader.PicklistItems[j].Picked) - (Convert.ToInt32(Math.Floor(pickingHeader.PicklistItems[j].FullPalletQuantity)) * pickingHeader.PicklistItems[j].UnitsPerPallet);
                            pickingHeader.PickedItems.Add(manifestLoadingStatuses[i]);
                            break;
                        }
                        // picked same amount as required
                        else if (pickingHeader.PicklistItems[j].RequiredQuantity - pickingHeader.PicklistItems[j].Picked
                                == manifestLoadingStatuses[i].PalletUnits)
                        {
                            pickingHeader.PickedItems.Add(manifestLoadingStatuses[i]);
                            pickingHeader.PicklistItems.Remove(pickingHeader.PicklistItems[j]);
                            break;
                        }
                        // picked more than required
                        else if (pickingHeader.PicklistItems[j].RequiredQuantity - pickingHeader.PicklistItems[j].Picked
                                < manifestLoadingStatuses[i].PalletUnits)
                        {
                            wrapper.Messages.Add("Picklist " + picklistNo.ToString() + ": Have picked more than required for product " + pickingHeader.PicklistItems[j].CatalogCode);
                            break;
                        }
                        else if (j == pickingHeader.PicklistItems.Count - 1)
                        {
                            wrapper.Messages.Add("Picklist " + picklistNo.ToString() + ": Product " + pickingHeader.PicklistItems[j].CatalogCode + " on pallet " + manifestLoadingStatuses[i].PalletNumber + " not found in order.");
                            break;
                        }
                    }
                }
            }

            wrapper = _pickingService.GetPickingSequenceDetails(pickingHeader.PicklistItems, warehouseCode, roomCode);
            if (wrapper.IsSuccess == false)
            {
                return wrapper;
            }

            pickingHeader.PicklistItems = wrapper.ResultSet[0] as List<PicklistItem>;
            ////pickingHeader.PicklistItems = pickingHeader.PicklistItems.OrderByDescending(s => s.BinLocation).ThenBy(s => s.PickingSequenceP).ThenBy(s => s.PickingSequenceB).ToList();
            //pickingHeader.PicklistItems = pickingHeader.PicklistItems.OrderBy(s => string.IsNullOrEmpty(s.BinLocation)).ThenBy(s => s.BinLocation).ThenBy(s => s.PickingSequenceP).ThenBy(s => s.PickingSequenceB).ToList();

            var picklistItems = pickingHeader.PicklistItems ?? new List<PicklistItem>();

            // 1. Items WITH BinLocation
            var withBinLocation = picklistItems
                .Where(s => !string.IsNullOrWhiteSpace(s.BinLocation))
                //.OrderBy(s => s.BinLocation)
                .OrderBy(s => s.PickingSequenceP)
                .ThenBy(s => s.PickingSequenceB)
                .ToList();

            // 2. Items WITHOUT BinLocation
            var withoutBinLocation = picklistItems
                .Where(s => string.IsNullOrWhiteSpace(s.BinLocation))
                .OrderBy(s => s.PickingSequenceP)
                .ThenBy(s => s.PickingSequenceB)
                .ToList();

            // 3. Combine (WithBin first, then WithoutBin)
            pickingHeader.PicklistItems = withBinLocation.Concat(withoutBinLocation).ToList();

            wrapper.IsSuccess = true;
            wrapper.ResultSet.Clear();
            wrapper.ResultSet.Add(pickingHeader);
            return wrapper;
        }

        public TransactionWrapper FinalisePicklist(string warehouseCode, string originator, string isTransfer, int manifestNo, int picklistNumber, int picklistItemsRem, int palletCount, int palletSpaces,
                                                   List<ManifestLoadingStatus> pickedPallets)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            using (TransactionScope scope = new TransactionScope())
            {
                try
                {
                    if (picklistItemsRem > 0) // check if there are any remaining items to be picked
                    {
                        PalletLocationLog palletLocationLog = new PalletLocationLog
                        {
                            Timestamp = DateTime.Now,
                            PalletNo = 0,
                            NewLocation = "FINALISING",
                            MovedBy = originator,
                            SyncTime = "",
                            ManifestNo = manifestNo,
                            Remark = "HH order incomplete! " + picklistItemsRem.ToString()
                        };

                        wrapper.IsSuccess = _pickingService.InsertPalletLocationLog(palletLocationLog);
                        if (wrapper.IsSuccess == false)
                        {
                            scope.Dispose();
                            wrapper.Messages.Add("InsertPalletLocationLog : Error");
                            return wrapper;
                        }
                    }

                    wrapper = _pickingService.GetPicklistPickedQuantity(manifestNo);

                    if (wrapper.IsSuccess == false)
                    {
                        scope.Dispose();
                        return wrapper;
                    }

                    List<ManifestLoadingStatus> manLoadStats = wrapper.ResultSet[0] as List<ManifestLoadingStatus>;
                    wrapper.ResultSet.Clear();

                    string overpicked = "";
                    for (int i = 0; i < manLoadStats.Count(); i++) // check if picked more than required
                    {
                        wrapper = _pickingService.GetOverpickedItems(manLoadStats[i]);
                        if (wrapper.IsSuccess == false)
                        {
                            scope.Dispose();
                            return wrapper;
                        }
                        else if (wrapper.ResultSet.Count() > 0)
                        {
                            overpicked += wrapper.ResultSet[0].ToString();
                        }
                    }

                    wrapper.ResultSet.Clear();

                    if (overpicked != "")
                    {
                        PalletLocationLog palletLocationLog = new PalletLocationLog
                        {
                            Timestamp = DateTime.Now,
                            PalletNo = manLoadStats[0].PicklistNumber,
                            NewLocation = "OVERPICKED",
                            MovedBy = originator,
                            SyncTime = "",
                            ManifestNo = manifestNo,
                            Remark = overpicked
                        };

                        wrapper.IsSuccess = _pickingService.InsertPalletLocationLog(palletLocationLog);
                        if (wrapper.IsSuccess == false)
                        {
                            scope.Dispose();
                            wrapper.Messages.Add("InsertPalletLocationLog : Error");
                            return wrapper;
                        }

                    }

                    for (int i = 0; i < manLoadStats.Count(); i++) // update amount picked
                    {
                        wrapper = _pickingService.UpdatePickedQuantity(manLoadStats[i]);
                        if (wrapper.IsSuccess == false)
                        {
                            scope.Dispose();
                            return wrapper;
                        }
                    }

                    if (isTransfer == "N") // save picklist 
                    {
                        string palletStatus = "D";
                        // string warehouseStatus = "E"; OpenROAD wants this variable but then uses it for a Select that it never uses
                        for (int i = 0; i < pickedPallets.Count(); i++)
                        {
                            float palletQuantity = 0;

                            wrapper = _pickingService.GetPalletQuantity(pickedPallets[i].PalletNumber);
                            if (wrapper.IsSuccess == false)
                            {
                                return wrapper;
                            }
                            palletQuantity = (float)wrapper.ResultSet[0];

                            int planNo = _pickingService.GetPlanNumber(pickedPallets[i].PalletNumber);
                            if (planNo == 0)
                            {
                                scope.Dispose();
                                wrapper.IsSuccess = false;
                                wrapper.Messages.Add("GetPlanNumber : Error");
                                return wrapper;
                            }
                            else if (planNo != -1) // non mixed pallet
                            {
                                wrapper = _pickingService.GetCatalogItemForPicking(pickedPallets[i].CatalogCode);
                                if (wrapper.IsSuccess == false)
                                {
                                    scope.Dispose();
                                    return wrapper;
                                }

                                CatalogItem catalogItem = wrapper.ResultSet[0] as CatalogItem;

                                if (catalogItem.PalletType == "T" || palletQuantity == 0)
                                {
                                    palletQuantity = 1;
                                }

                                wrapper = _pickingService.FinaliseNonMixPalletDetail(pickedPallets[i], manifestNo, warehouseCode, palletQuantity);
                                if (wrapper.IsSuccess == false)
                                {
                                    scope.Dispose();
                                    return wrapper;
                                }
                            }
                            else if (planNo == -1) // mix pallet
                            {
                                wrapper = _pickingService.FinaliseMixPalletDetail(pickedPallets[i], manifestNo, warehouseCode);
                                if (wrapper.IsSuccess == false)
                                {
                                    scope.Dispose();
                                    return wrapper;
                                }

                                if (pickedPallets[i].PicklistNumber != 0 && manifestNo != 0 && warehouseCode != "")
                                {
                                    wrapper = _pickingService.UpdatePalletDetailWithZeroValue(pickedPallets[i], manifestNo, warehouseCode);
                                    if (wrapper.IsSuccess == false)
                                    {
                                        scope.Dispose();
                                        return wrapper;
                                    }
                                }
                            }

                            wrapper = _pickingService.UpdatePalletHeaderStatus(palletStatus, pickedPallets[i].PalletNumber);
                            if (wrapper.IsSuccess == false)
                            {
                                scope.Dispose();
                                return wrapper;
                            }
                            /*
                            wrapper = _pickingService.UpdatePalletMovementInfo(palletStatus, pickedPallets[i].PalletNumber);
                            if (wrapper.IsSuccess == false)
                            {
                                scope.Dispose();
                                return wrapper;
                            }*/
                        }

                        wrapper = _pickingService.UpdatePickerAllocationStatus("X", manifestNo, picklistNumber, originator);
                        if (wrapper.IsSuccess == false)
                        {
                            scope.Dispose();
                            return wrapper;
                        }

                        PalletLocationLog palletLocationLog = new PalletLocationLog
                        {
                            Timestamp = DateTime.Now,
                            PalletNo = 0,
                            NewLocation = "FINALISED",
                            MovedBy = originator,
                            SyncTime = "",
                            ManifestNo = manifestNo,
                            Remark = "HH plist " + picklistNumber
                        };

                        wrapper.IsSuccess = _pickingService.InsertPalletLocationLog(palletLocationLog);
                        if (wrapper.IsSuccess == false)
                        {
                            wrapper.Messages.Add("InsertPalletLocationLog : Error");
                            return wrapper;
                        }
                    }
                    else if (isTransfer == "Y")
                    {
                        wrapper = _pickingService.GetTransfer(manifestNo);
                        if (wrapper.IsSuccess == false)
                        {
                            scope.Dispose();
                            return wrapper;
                        }

                        string WHto = "";
                        if (wrapper.ResultSet.Count() > 0)
                        {
                            WHto = wrapper.ResultSet[0] as string;
                            if (WHto == "")
                            {
                                scope.Dispose();
                                wrapper.IsSuccess = false;
                                wrapper.Messages.Add("No transfer found for manifest " + manifestNo);
                                return wrapper;
                            }
                        }

                        for (int i = 0; i < pickedPallets.Count(); i++)
                        {
                            wrapper = _pickingService.UpdatePalletDetailWarehouse(WHto, pickedPallets[i].PalletNumber, pickedPallets[i].CatalogCode);
                            if (wrapper.IsSuccess == false)
                            {
                                scope.Dispose();
                                return wrapper;
                            }

                            wrapper = _pickingService.UpdatePalletHeaderWarehouse("C7", "P", pickedPallets[i].PalletNumber);
                            if (wrapper.IsSuccess == false)
                            {
                                scope.Dispose();
                                return wrapper;
                            }

                            wrapper = _pickingService.UpdateManifestStatus("D", manifestNo);
                            if (wrapper.IsSuccess == false)
                            {
                                scope.Dispose();
                                return wrapper;
                            }
                        }

                        wrapper = _pickingService.UpdatePickerAllocationStatus("X", manifestNo, pickedPallets[0].PicklistNumber, originator);
                        if (wrapper.IsSuccess == false)
                        {
                            scope.Dispose();
                            return wrapper;
                        }

                        PalletLocationLog palletLocationLog = new PalletLocationLog
                        {
                            Timestamp = DateTime.Now,
                            PalletNo = 0,
                            NewLocation = "FINALISED",
                            MovedBy = originator,
                            SyncTime = "",
                            ManifestNo = manifestNo,
                            Remark = "HH TX plist " + pickedPallets[0].PicklistNumber
                        };

                        wrapper.IsSuccess = _pickingService.InsertPalletLocationLog(palletLocationLog);
                        if (wrapper.IsSuccess == false)
                        {
                            scope.Dispose();
                            return wrapper;
                        }
                    }

                    //if (palletCount > 0 || palletSpaces > 0)
                    //{
                    //    wrapper = _pickingService.UpdatePlistPalletCountAndSpaces(picklistNumber, palletCount, palletSpaces);
                    //    if (wrapper.IsSuccess == false)
                    //    {
                    //        scope.Dispose();
                    //        return wrapper;
                    //    }
                    //}

                    scope.Complete();
                    wrapper.IsSuccess = true;
                }
                catch (Exception e)
                {
                    scope.Dispose();
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add(e.Message);
                    return wrapper;
                }
            }
            
            return wrapper;
        }

        public TransactionWrapper GetNegativePickBin()
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            int NoNegativePickBin = _pickingService.GetNegativePickBin();
            if (NoNegativePickBin == -1)
            {
                wrapper.IsSuccess = false;
                wrapper.Messages.Add("GetNegativePickBin : Error");
                return wrapper;
            }
            else
            {
                wrapper.IsSuccess = true;
                wrapper.ResultSet.Add(NoNegativePickBin);
                return wrapper;
            }
        }

        public TransactionWrapper OpenNewPallet(string originator, string warehouseCode, int manifestNo, int picklistNo)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            int newPalletNo = 0;
            using (TransactionScope scope = new TransactionScope())
            {
                try
                {
                    newPalletNo = _pickingService.GetNewPalletNumber();
                    if (newPalletNo == 0)
                    {
                        scope.Dispose();
                        wrapper.IsSuccess = false;
                        wrapper.Messages.Add("Error: Could not get unique number for new pallet");
                        return wrapper;
                    }

                    PalletHeader pallet = new PalletHeader
                    {
                        PalletNumber = newPalletNo,
                        PrintedAt = "HH-" + originator,
                        PrintDate = DateTime.Now,
                        PlanNumber = -1,
                        TransferStatus = "T",
                        WarehouseId = warehouseCode,
                        Status = "P",
                        Quality = "G",
                        BinLocation = "PICKED",
                    };

                    wrapper = _pickingService.InsertNewPallet(pallet);
                    if (wrapper.IsSuccess == false)
                    {
                        scope.Dispose();
                        return wrapper;
                    }

                    wrapper = _pickingService.SetOpenPalletNumber(manifestNo, picklistNo, newPalletNo, originator);
                    if (wrapper.IsSuccess == false)
                    {
                        scope.Dispose();
                        return wrapper;
                    }

                    PalletLocationLog palletLocationLog = new PalletLocationLog
                    {
                        Timestamp = DateTime.Now,
                        PalletNo = newPalletNo,
                        NewLocation = "STARTED",
                        MovedBy = originator,
                        SyncTime = "",
                        ManifestNo = manifestNo,
                        Remark = "HH picking for " + picklistNo.ToString()
                    };

                    bool isSuccess = _pickingService.InsertPalletLocationLog(palletLocationLog);
                    if (!isSuccess)
                    {
                        scope.Dispose();
                        wrapper.IsSuccess = isSuccess;
                        wrapper.Messages.Add("InsertPalletLocationLog : Error");
                        return wrapper;
                    }

                    scope.Complete();
                    wrapper.IsSuccess = true;
                    wrapper.ResultSet.Add(newPalletNo);
                }
                catch (Exception e)
                {
                    scope.Dispose();
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add(e.Message);
                    return wrapper;
                }
            }

            return wrapper;
        }

        public TransactionWrapper RemovePicked(ManifestLoadingStatus manLoadStatus, string warehouseId, string roomCode, string originator)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            int originalPalletUnits = 0;
            double originalPalletQuantity = 0;

            using (TransactionScope scope = new TransactionScope())
            {
                try
                {
                    wrapper = _pickingService.GetCatalogItemForPicking(manLoadStatus.CatalogCode);
                    if (wrapper.IsSuccess == false)
                    {
                        scope.Dispose();
                        return wrapper;
                    }

                    CatalogItem catalogItem = wrapper.ResultSet[0] as CatalogItem;

                    wrapper = _pickingService.GetUom(catalogItem.UomPallet);
                    if (wrapper.IsSuccess == false)
                    {
                        scope.Dispose();
                        return wrapper;
                    }

                    Uom uom;
                    if (wrapper.ResultSet.Count > 0)
                    {
                        uom = wrapper.ResultSet[0] as Uom;
                        if (uom.Conversion != 0)
                        {
                            originalPalletUnits = manLoadStatus.PalletUnits;
                            originalPalletQuantity = manLoadStatus.PalletUnits / uom.Conversion;
                        }
                    }

                    wrapper = _pickingService.InsertRemovedPallets(manLoadStatus, originator);
                    if (wrapper.IsSuccess == false)
                    {
                        scope.Dispose();
                        return wrapper;
                    }

                    wrapper = _pickingService.GetPalletPrintedAt(manLoadStatus.PalletNumber);
                    if (wrapper.IsSuccess == false)
                    {
                        scope.Dispose();
                        return wrapper;
                    }

                    string printedAt = wrapper.ResultSet[0] as string;
                    if (printedAt.Contains("HH-"))
                    {
                        wrapper = _pickingService.UpdatePalletDetailUnpicked(manLoadStatus, warehouseId);
                        if (wrapper.IsSuccess == false)
                        {
                            scope.Dispose();
                            return wrapper;
                        }
                    }
                    else
                    {
                        wrapper = _pickingService.UpdatePalletHeaderUnpicked(warehouseId, roomCode, manLoadStatus.PalletNumber);
                        if (wrapper.IsSuccess == false)
                        {
                            scope.Dispose();
                            return wrapper;
                        }
                    }

                    wrapper = _pickingService.DeleteManifestLoadingStatus(manLoadStatus);
                    if (wrapper.IsSuccess == false)
                    {
                        scope.Dispose();
                        return wrapper;
                    }

                    PalletLocationLog palletLocationLog = new PalletLocationLog
                    {
                        Timestamp = DateTime.Now,
                        PalletNo = manLoadStatus.PalletNumber,
                        NewLocation = warehouseId + "." + roomCode + ".UNPICKED",
                        MovedBy = originator,
                        SyncTime = "",
                        ManifestNo = manLoadStatus.ManifestNumber,
                        Remark = manLoadStatus.CatalogCode + " (" + manLoadStatus.PalletUnits.ToString() + ") BB: " + manLoadStatus.BestBefore.ToString()
                    };

                    wrapper.IsSuccess = _pickingService.InsertPalletLocationLog(palletLocationLog);
                    if (wrapper.IsSuccess == false)
                    {
                        scope.Dispose();
                        return wrapper;
                    }

                    scope.Complete();
                    wrapper.IsSuccess = true;
                }
                catch (Exception e)
                {
                    scope.Dispose();
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add(e.Message);
                    return wrapper;
                }
            }

            return wrapper;
        }

        public TransactionWrapper ScanLabel(string scanData, string warehouseCode, string roomCode, int plistNo)
        {
            try
            {
                TransactionWrapper wrapper = new TransactionWrapper();
                TransactionWrapper wrapperQP = new TransactionWrapper();

                bool isPallet = false;
                int palletNo = 0;
                string rackLocation = "";
                string letter = scanData.Substring(scanData.Length - 1, 1);
                string availableLetters = "ABCDEFGHK";
                if (scanData.Length == 4 && availableLetters.Contains(letter))
                {
                    rackLocation = warehouseCode + "." + roomCode + "." + scanData;
                }
                else if (scanData.Contains('.'))
                {
                    string[] rackParts = scanData.Split('.');
                    if (rackParts.Count() != 3)
                    {
                        wrapper.IsSuccess = false;
                        wrapper.Messages.Add("Invalid scan");
                        return wrapper;
                    }
                    else
                    {
                        rackLocation = rackParts[0] + "." + rackParts[1] + "." + rackParts[2];
                    }
                }

                if (rackLocation == "")
                {
                    isPallet = true;
                    if (scanData[0] == '0')
                    {
                        wrapper = _pickingService.GetPalletViaPickingLabel(scanData);
                        if (wrapper.IsSuccess == false)
                        {
                            return wrapper;
                        }
                        else if (wrapper.ResultSet.Count > 0)
                        {
                            palletNo = Convert.ToInt32(wrapper.ResultSet[0]);
                        }
                    }
                }
                else // check if one pallet in location
                {
                    // check if scanned location is a pallet
                    wrapper = _pickingService.GetPalletCountInLocation(rackLocation);
                    if (wrapper.IsSuccess == false)
                    {
                        return wrapper;
                    }
                    else if (wrapper.ResultSet.Count > 0)
                    {
                        isPallet = true;
                        palletNo = Convert.ToInt32(wrapper.ResultSet[0]);
                    }
                }

                if (isPallet) // scanned a pallet
                {
                    if (palletNo == 0)
                    {
                        palletNo = Int32.Parse(scanData);
                    }
                    wrapper = _pickingService.GetCatalogCodeFromPalletDetail(palletNo);
                    if (wrapper.IsSuccess == false)
                    {
                        return wrapper;
                    }
                    else if (wrapper.ResultSet.Count > 1)
                    {
                        wrapper.IsSuccess = false;
                        wrapper.Messages.Add("Too many lines in pallet detail");
                        return wrapper;
                    }

                    string catalogCode = Convert.ToString(wrapper.ResultSet[0]);

                    wrapper = _pickingService.GetForkliftPallet(palletNo);
                    if (wrapper.IsSuccess == false)
                    {
                        return wrapper;
                    }

                    PalletHeader pallet = wrapper.ResultSet[0] as PalletHeader;
                    wrapper.ResultSet.Clear();

                    if (pallet.Status == "D")
                    {
                        wrapper.IsSuccess = false;
                        wrapper.Messages.Add("Pallet #" + palletNo.ToString() + " has already been DESPATCHED");
                        return wrapper;
                    }
                    else if (pallet.Quality == "R")
                    {
                        wrapper.IsSuccess = false;
                        wrapper.Messages.Add("Pallet #" + palletNo.ToString() + " has been REJECTED");
                        return wrapper;
                    }
                    else if (pallet.TransferStatus != "T")
                    {
                        wrapper.IsSuccess = false;
                        wrapper.Messages.Add("Pallet #" + palletNo.ToString() + " has not been RELEASED");
                        return wrapper;
                    }

                    ////Get All UseBy Date for Plist Number and Catalog Code
                    //wrapper = _pickingService.GetUseByDateByPlistNoAndCatCode(plistNo, catalogCode);
                    //if (wrapper.IsSuccess == true)
                    //{
                    //    List<UseByDates> useByDates = wrapper.ResultSet[0] as List<UseByDates>;

                    //    // Check if the list is not empty
                    //    if (useByDates.Any())
                    //    {
                    //        // Get the minimum UseByDate
                    //        DateTime minUseByDate = useByDates.Min(date => date.UseByDate);

                    //        // If Best before date less than minimum use by date then send error
                    //        if (pallet.PalletDetails[0].BestBefore < minUseByDate)
                    //        {
                    //            wrapper.IsSuccess = false;
                    //            wrapper.Messages.Add("This catalog code best before date less than use by date.");
                    //            return wrapper;
                    //        }
                    //    }
                    //}

                    // Here is where the original application would check the pallet table in the view for if this pallet existed. This should
                    // now be done prior to making the API request.

                    wrapper = _pickingService.GetManifestLoadingStatusCount(palletNo);
                    if (wrapper.IsSuccess == false)
                    {
                        return wrapper;
                    }

                    wrapper = _pickingService.GetCatalogItemForPicking(catalogCode);
                    if (wrapper.IsSuccess == false)
                    {
                        return wrapper;
                    }

                    pallet.PalletDetails[0].CatalogItem = wrapper.ResultSet[0] as CatalogItem;
                    wrapper.ResultSet.Clear();

                    if (DateTime.Now.AddDays(pallet.PalletDetails[0].CatalogItem.ShelfLife) >= pallet.PalletDetails[0].BestBefore)
                    {
                        wrapper.IsSuccess = false;
                        wrapper.Messages.Add("Pallet " + palletNo.ToString() + " expires before shelf life");
                        return wrapper;
                    }

                    //Added By Irosh 2025/02/19
                    wrapperQP = _palletService.GetPalletHeaderByPalletNo(palletNo);
                    if (wrapperQP.IsSuccess == false)
                    {
                        return wrapperQP;
                    }

                    PalletHeader palletHeader = wrapperQP.ResultSet[0] as PalletHeader;
                    if (palletHeader != null && palletHeader.WarehouseId != null)
                    {
                        if (palletHeader.BinLocation.Contains("E1"))
                        {
                            if (palletHeader.WarehouseId == "QP" || palletHeader.WarehouseId == "QR")
                            {
                                wrapperQP.IsSuccess = false;
                                wrapperQP.Messages.Add("Invalid WarehouseId: QP or QR are not allowed.");
                                return wrapperQP;
                            }
                        }
                    }
                    //End Added By Irosh 2025/02/19

                    wrapper.IsSuccess = true;
                    wrapper.ResultSet.Add(pallet);
                    return wrapper;
                }
                else //scanned a rack
                {
                    string[] rackSections = rackLocation.Split('.');
                    if (rackSections.Count() != 3)
                    {
                        wrapper.IsSuccess = false;
                        wrapper.Messages.Add("Scan data is in an invalid format");
                        return wrapper;
                    }

                    wrapper = _pickingService.GetWarehouseRack(rackSections[0], rackSections[1], rackSections[2]);
                    if (wrapper.IsSuccess == false)
                    {
                        return wrapper;
                    }

                    WarehouseRack rack = wrapper.ResultSet[0] as WarehouseRack;
                    wrapper.ResultSet.Clear();

                    if (!String.IsNullOrEmpty(rack.ReservedCatalogCode))
                    {
                        wrapper = _pickingService.GetCatalogItemForPicking(rack.ReservedCatalogCode);
                        if (wrapper.IsSuccess == false)
                        {
                            return wrapper;
                        }
                    }
                    else
                    {
                        wrapper.IsSuccess = false;
                        wrapper.Messages.Add("Rack location " + scanData + " has no reserved catalog code");
                        return wrapper;
                    }
                    CatalogItem catalogItem = wrapper.ResultSet[0] as CatalogItem;
                    wrapper.ResultSet.Clear();

                    int count = _pickingService.CheckPalletShelfLife(rack.LicensedPalletNo, rack.ReservedCatalogCode, catalogItem.ShelfLife);
                    if (count == -1)
                    {
                        wrapper.IsSuccess = false;
                        wrapper.Messages.Add("CheckPalletShelfLife : Error");
                        return wrapper;
                    }
                    else if (count > 0)
                    {
                        wrapper.IsSuccess = false;
                        wrapper.Messages.Add("Rack " + scanData + " has short shelf life items. Please choose units expiring after " + DateTime.Now.AddDays(catalogItem.ShelfLife).ToString());
                    }

                    float availablePickingQty = _pickingService.GetAvailablePickingQuantity(rack.LicensedPalletNo, rack.ReservedCatalogCode, catalogItem.ShelfLife);
                    if (wrapper.IsSuccess == false)
                    {
                        wrapper.Messages.Add("Could not get available picking quantity");
                        return wrapper;
                    }

                    //Added By Irosh 2025/02/19
                    wrapperQP = _palletService.GetPalletHeaderByPalletNo(rack.LicensedPalletNo);
                    if (wrapperQP.IsSuccess == false)
                    {
                        return wrapperQP;
                    }

                    PalletHeader palletHeader = wrapperQP.ResultSet[0] as PalletHeader;
                    if (palletHeader != null && palletHeader.WarehouseId != null)
                    {
                        if (palletHeader.BinLocation.Contains("E1"))
                        {
                            if (palletHeader.WarehouseId == "QP" || palletHeader.WarehouseId == "QR")
                            {
                                wrapperQP.IsSuccess = false;
                                wrapperQP.Messages.Add("Invalid WarehouseId: QP or QR are not allowed.");
                                return wrapperQP;
                            }
                        }
                    }
                    //End Added By Irosh 2025/02/19

                    wrapper.IsSuccess = true;
                    wrapper.ResultSet.Add(availablePickingQty);
                    wrapper.ResultSet.Add(catalogItem);
                    return wrapper;
                }
            }
            catch (Exception e)
            {
                TransactionWrapper wrapper = new TransactionWrapper();
                wrapper.IsSuccess = false;
                wrapper.Messages.Add(e.Message);
                return wrapper;
            }
        }

        public TransactionWrapper ScanPickingLabel(string pickingLabel, string originator, int palletNo, int manifestNo, int picklistNo)
        {
            TransactionWrapper wrapper = new TransactionWrapper();

            int count = _pickingService.GetPickingLabelCount(pickingLabel);
            if (count == -1)
            {
                wrapper.IsSuccess = false;
                wrapper.Messages.Add("GetPickingLabelCount : Error");
                return wrapper;
            }

            if (count > 0)
            {
                wrapper.IsSuccess = false;
                wrapper.Messages.Add("Picking label " + pickingLabel + " has been used within the last year. Cannot reuse it");
                return wrapper;
            }

            using (TransactionScope scope = new TransactionScope())
            {
                try
                {
                    wrapper = _pickingService.UpdatePalletHeaderPickingLabel(pickingLabel, palletNo);
                    if (wrapper.IsSuccess == false)
                    {
                        scope.Dispose();
                        return wrapper;
                    }

                    wrapper = _pickingService.SetOpenPalletNumber(manifestNo, picklistNo, 0, originator);
                    if (wrapper.IsSuccess == false)
                    {
                        scope.Dispose();
                        return wrapper;
                    }

                    PalletLocationLog palletLocationLog = new PalletLocationLog
                    {
                        Timestamp = DateTime.Now,
                        PalletNo = palletNo,
                        NewLocation = "CLOSED",
                        MovedBy = originator,
                        SyncTime = "",
                        ManifestNo = manifestNo,
                        Remark = "HH picking Lbl " + pickingLabel
                    };

                    wrapper.IsSuccess = _pickingService.InsertPalletLocationLog(palletLocationLog);
                    if (wrapper.IsSuccess == false)
                    {
                        scope.Dispose();
                        wrapper.Messages.Add("InsertPalletLocationLog : Error");
                        return wrapper;
                    }

                    scope.Complete();
                    wrapper.IsSuccess = true;
                }
                catch (Exception e)
                {
                    scope.Dispose();
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add(e.Message);
                    return wrapper;
                }
            }

            return wrapper;
        }

        public TransactionWrapper GetTransitWarehouses(string fromWh, string toWh)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            PickingHeader pickingHeader = new PickingHeader();

            // get picklist items
            wrapper = _pickingService.GetTransitWarehouses(fromWh, toWh);
            if (wrapper.IsSuccess == false)
            {
                return wrapper;
            }

            wrapper.IsSuccess = true;
            return wrapper;
        }
    }
}
