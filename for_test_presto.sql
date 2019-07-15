WITH tmp AS (
		SELECT row_number() OVER () AS RW, *
		FROM (
			SELECT coalesce(sum(salesQty), NULL) AS salesQty
				, coalesce(sum(vcardAmount), NULL) AS vcardAmount
				, coalesce(sum(overseasTax), NULL) AS overseasTax
				, coalesce(sum(marketCost), NULL) AS marketCost
				, coalesce(sum(technicalCost), NULL) AS technicalCost
				, coalesce(sum(adminCost), NULL) AS adminCost
				, coalesce(sum(departmentCost), NULL) AS departmentCost
				, round(SUM(CAST(marketCost AS DOUBLE)) / SUM(CAST(originNetSalesMoney AS DOUBLE)), 8) AS marketCostPercent
				, round(SUM(CAST(netSalesCost AS DOUBLE)), 8) AS comprehensiveSalesCost
				, round(SUM(CAST(originNetSalesMoney AS DOUBLE)), 8) AS comprehensiveSalesMoney
				, round((SUM(CAST(marketCost AS DOUBLE)) + SUM(CAST(technicalCost AS DOUBLE)) + SUM(CAST(adminCost AS DOUBLE)) + SUM(CAST(departmentCost AS DOUBLE)) + SUM(CAST(businessSupportCost AS DOUBLE)) + SUM(CAST(customerServiceCost AS DOUBLE))) / SUM(CAST(originNetSalesMoney AS DOUBLE)), 8) AS indirectExpensesRate
				, round(SUM(CAST(customerServiceCost AS DOUBLE)) / SUM(CAST(originNetSalesMoney AS DOUBLE)), 8) AS customerServiceCostPercent
				, round(SUM(CAST(businessSupportCost AS DOUBLE)) / SUM(CAST(originNetSalesMoney AS DOUBLE)), 8) AS businessSupportCostPercent
				, round(SUM(CAST(departmentCost AS DOUBLE)) / SUM(CAST(originNetSalesMoney AS DOUBLE)), 8) AS departmentCostPercent
				, round(SUM(CAST(adminCost AS DOUBLE)) / SUM(CAST(originNetSalesMoney AS DOUBLE)), 8) AS adminCostPercent
				, round(SUM(CAST(technicalCost AS DOUBLE)) / SUM(CAST(originNetSalesMoney AS DOUBLE)), 8) AS technicalCostPercent
				, round(SUM(CAST(netSalesCostTax AS DOUBLE)) / SUM(CAST(originNetSalesMoney AS DOUBLE)), 8) AS netSalesCostTaxPercant
				, round(SUM(CAST(netSalesCostTax AS DOUBLE)) / SUM(CAST(netSalesMoneyTax AS DOUBLE)), 8) AS netSalesCostTaxPercent
				, coalesce(sum(netSalesCostTax), NULL) AS netSalesCostTax
				, round(SUM(CAST(netSalesMoneyTax AS DOUBLE)) / SUM(CAST(netSalesTotalMoneyTax AS DOUBLE)), 8) AS netSalesMoneyTaxPercent
				, coalesce(sum(salesCost), NULL) AS salesCost
				, coalesce(sum(salesCostTax), NULL) AS salesCostTax
				, coalesce(sum(salesMoney), NULL) AS salesMoney
				, coalesce(sum(salesMoneyTax), NULL) AS salesMoneyTax
				, round(SUM(CAST(costAdjustMoneyCny AS DOUBLE)) / SUM(CAST(originNetSalesMoney AS DOUBLE)), 8) AS indicator_2
				, round(SUM(CAST(otherIncomeMoneyCny AS DOUBLE)) / SUM(CAST(originNetSalesMoney AS DOUBLE)), 8) AS indicator_3
				, round(SUM(CAST(otherIncomeMoneyCnyTax AS DOUBLE)) / SUM(CAST(originNetSalesMoney AS DOUBLE)), 8) AS indicator_4
				, round(SUM(CAST(costAdjustMoneyCnyTax AS DOUBLE)) / SUM(CAST(originNetSalesMoney AS DOUBLE)), 8) AS indicator_5
				, coalesce(sum(netProfit), NULL) AS netProfit
				, coalesce(sum(estimateStepRebateAmountTax), NULL) AS estimateStepRebateAmountTax
				, coalesce(sum(incomingDifferenceAmountTax), NULL) AS incomingDifferenceAmountTax
				, coalesce(sum(returnDiscountsAmountTax), NULL) AS returnDiscountsAmountTax
				, coalesce(sum(estimateSalesRebateAmount), NULL) AS estimateSalesRebateAmount
				, coalesce(sum(ReturnSubsidyAmount), NULL) AS ReturnSubsidyAmount
				, coalesce(sum(SalesRebateAmount), NULL) AS SalesRebateAmount
				, coalesce(sum(discountAmount), NULL) AS discountAmount
				, coalesce(sum(estimateStepRebateAmount), NULL) AS estimateStepRebateAmount
				, coalesce(sum(netSalesTotalMoneyTax), NULL) AS netSalesTotalMoneyTax
				, coalesce(sum(customerServiceCost), NULL) AS customerServiceCost
				, coalesce(sum(costAdjustMoneyCnyTax), NULL) AS costAdjustMoneyCnyTax
				, coalesce(sum(nationalInspectionAmountTax), NULL) AS nationalInspectionAmountTax
				, coalesce(sum(netSalesCost), NULL) AS netSalesCost
				, coalesce(sum(netSalesMoneyTax), NULL) AS netSalesMoneyTax
				, coalesce(sum(freightIncomeCny), NULL) AS freightIncomeCny
				, coalesce(sum(GrossSubsidyAmount), NULL) AS GrossSubsidyAmount
				, coalesce(sum(settlServiceAndTax), NULL) AS settlServiceAndTax
				, coalesce(sum(PriceAdjustAmount), NULL) AS PriceAdjustAmount
				, coalesce(sum(PriceAdjustAmountTax), NULL) AS PriceAdjustAmountTax
				, coalesce(sum(freightCostCny), NULL) AS freightCostCny
				, coalesce(sum(otherIncomeMoneyCnyTax), NULL) AS otherIncomeMoneyCnyTax
				, coalesce(sum(ReturnSubsidyAmountTax), NULL) AS ReturnSubsidyAmountTax
				, coalesce(sum(brandPromotionAmountTax), NULL) AS brandPromotionAmountTax
				, coalesce(sum(indirectExpenses), NULL) AS indirectExpenses
				, coalesce(sum(busiDiscountAmountTax), NULL) AS busiDiscountAmountTax
				, coalesce(sum(vendorDiscountAmountTax), NULL) AS vendorDiscountAmountTax
				, coalesce(sum(otdStationOutAmount), NULL) AS otdStationOutAmount
				, coalesce(sum(salesOrderTotalQty), NULL) AS salesOrderTotalQty
				, coalesce(sum(generalTradeAmount), NULL) AS generalTradeAmount
				, coalesce(sum(otdStationInAmountTax), NULL) AS otdStationInAmountTax
				, coalesce(sum(SalesRebateAmountTax), NULL) AS SalesRebateAmountTax
				, coalesce(sum(otherTotalDiscountAmount), NULL) AS otherTotalDiscountAmount
				, coalesce(sum(vendorDiscountsAmountTax), NULL) AS vendorDiscountsAmountTax
				, coalesce(sum(nationalInspectionAmount), NULL) AS nationalInspectionAmount
				, coalesce(sum(estimateReturnSubsidyAmount), NULL) AS estimateReturnSubsidyAmount
				, coalesce(sum(costAdjustmentAmount), NULL) AS costAdjustmentAmount
				, coalesce(sum(estimatePriceAdjustAmountTax), NULL) AS estimatePriceAdjustAmountTax
				, coalesce(sum(costAdjustMoneyCny), NULL) AS costAdjustMoneyCny
				, coalesce(sum(costAdjustmentAmountTax), NULL) AS costAdjustmentAmountTax
				, coalesce(sum(estimateReturnSubsidyAmountTax), NULL) AS estimateReturnSubsidyAmountTax
				, coalesce(sum(StepRebateAmountTax), NULL) AS StepRebateAmountTax
				, coalesce(sum(vendorDiscountsAmount), NULL) AS vendorDiscountsAmount
				, coalesce(sum(incomingDifferenceAmount), NULL) AS incomingDifferenceAmount
				, coalesce(sum(StepRebateAmount), NULL) AS StepRebateAmount
				, coalesce(sum(returnDiscountsAmount), NULL) AS returnDiscountsAmount
				, coalesce(sum(netSalesMoney), NULL) AS netSalesMoney
				, coalesce(sum(generalTradeAmountTax), NULL) AS generalTradeAmountTax
				, coalesce(sum(brandPromotionAmount), NULL) AS brandPromotionAmount
				, coalesce(sum(otdStationInAmount), NULL) AS otdStationInAmount
				, coalesce(sum(otdStationOutAmountTax), NULL) AS otdStationOutAmountTax
				, coalesce(sum(businessSupportCost), NULL) AS businessSupportCost
				, coalesce(sum(inventorySubsidyAmount), NULL) AS inventorySubsidyAmount
				, coalesce(sum(vendorDiscountAmount), NULL) AS vendorDiscountAmount
				, coalesce(sum(estimateSalesRebateAmountTax), NULL) AS estimateSalesRebateAmountTax
				, coalesce(sum(originNetSalesMoney), NULL) AS originNetSalesMoney
				, coalesce(sum(estimatePriceAdjustAmount), NULL) AS estimatePriceAdjustAmount
				, coalesce(sum(estimateGrossSubsidyAmount), NULL) AS estimateGrossSubsidyAmount
				, coalesce(sum(otherIncomeMoneyCny), NULL) AS otherIncomeMoneyCny
				, coalesce(sum(memberReturnAmount), NULL) AS memberReturnAmount
				, coalesce(sum(tailnetSalesTotalMoneyTax), NULL) AS tailnetSalesTotalMoneyTax
				, coalesce(sum(memberReturnAmountTax), NULL) AS memberReturnAmountTax
				, coalesce(sum(GrossSubsidyAmountTax), NULL) AS GrossSubsidyAmountTax
				, round(SUM(CAST(originNetSalesMoney AS DOUBLE)) / SUM(CAST(netSalesTotalMoneyTax AS DOUBLE)), 8) AS originNetSalesMoneyPercent
				, round(SUM(CAST(netSalesCost AS DOUBLE)) / SUM(CAST(originNetSalesMoney AS DOUBLE)), 8) AS netSalesCostPercent
				, round(SUM(CAST(freightCostCny AS DOUBLE)) / SUM(CAST(originNetSalesMoney AS DOUBLE)), 8) AS freightCostCnyPercent
				, round(SUM(CAST(settlServiceAndTax AS DOUBLE)) / SUM(CAST(originNetSalesMoney AS DOUBLE)), 8) AS settlServiceAndTaxPercent
				, round(SUM(CAST(overseasTax AS DOUBLE)) / SUM(CAST(originNetSalesMoney AS DOUBLE)), 8) AS overseasTaxPercent
				, coalesce(sum(estimateRebateSubsidy), NULL) AS estimateRebateSubsidy
				, coalesce(sum(estimateRebateSubsidyTax), NULL) AS estimateRebateSubsidyTax
				, coalesce(sum(rebateSubsidyTax), NULL) AS rebateSubsidyTax
				, coalesce(sum(rebateSubsidy), NULL) AS rebateSubsidy
				, coalesce(sum(grossProfitEstimateSubsidy), NULL) AS grossProfitEstimateSubsidy
				, coalesce(sum(grossProfitSubsidy), NULL) AS grossProfitSubsidy
				, coalesce(sum(estimateGrossSubsidyAmountTax), NULL) AS estimateGrossSubsidyAmountTax
				, coalesce(sum(grossProfit), NULL) AS grossProfit
				, coalesce(sum(netProfitEstimateSubsidy), NULL) AS netProfitEstimateSubsidy
				, coalesce(sum(netProfitSubsidy), NULL) AS netProfitSubsidy
				, round(SUM(CAST(netProfit AS DOUBLE)) / SUM(CAST(originNetSalesMoney AS DOUBLE)), 8) AS netMargin
				, round(SUM(CAST(grossProfit AS DOUBLE)) / SUM(CAST(netSalesMoneyTax AS DOUBLE)), 8) AS indicator_16
				, round(SUM(CAST(grossProfitEstimateSubsidy AS DOUBLE)) / SUM(CAST(netSalesMoneyTax AS DOUBLE)), 8) AS grossMarginEstimateSubsidy
				, round(SUM(CAST(grossProfitSubsidy AS DOUBLE)) / SUM(CAST(netSalesMoneyTax AS DOUBLE)), 8) AS grossMarginSubsidy
				, round(SUM(CAST(netProfitEstimateSubsidy AS DOUBLE)) / SUM(CAST(originNetSalesMoney AS DOUBLE)), 8) AS netMarginEstimateSubsidy
				, round(SUM(CAST(netProfitSubsidy AS DOUBLE)) / SUM(CAST(originNetSalesMoney AS DOUBLE)), 8) AS netMarginSubsidy
				, round(SUM(CAST(rebateSubsidy AS DOUBLE)) / SUM(CAST(originNetSalesMoney AS DOUBLE)), 8) AS rebateSubsidyPercent
				, round(SUM(CAST(rebateSubsidyTax AS DOUBLE)) / SUM(CAST(netSalesMoney AS DOUBLE)), 8) AS rebateSubsidyTaxPercent
			FROM (
				SELECT dt, hour, salesOrderTotalQty, salesQty, netSalesTotalMoneyTax
					, salesMoney, salesMoneyTax, netSalesMoney, netSalesMoneyTax, originNetSalesMoney
					, tailnetSalesTotalMoneyTax, salesCost, salesCostTax, netSalesCost, netSalesCostTax
					, busiDiscountAmountTax, discountAmount, otherTotalDiscountAmount, vendorDiscountAmount, vendorDiscountAmountTax
					, overseasTax, freightIncomeCny, freightCostCny, vcardAmount, settlServiceAndTax
					, marketCost, technicalCost, adminCost, departmentCost, businessSupportCost
					, customerServiceCost
					, marketCost + technicalCost + adminCost + departmentCost + businessSupportCost + customerServiceCost AS indirectExpenses
					, costAdjustMoneyCny, costAdjustMoneyCnyTax, otherIncomeMoneyCny, otherIncomeMoneyCnyTax
					, netSalesMoneyTax - netSalesCostTax - overseasTax + otherIncomeMoneyCnyTax - costAdjustMoneyCnyTax AS grossProfit
					, netSalesMoneyTax - netSalesCostTax - overseasTax + otherIncomeMoneyCnyTax - costAdjustMoneyCnyTax + (+estimateSalesRebateAmountTax + estimateGrossSubsidyAmountTax + estimateReturnSubsidyAmountTax + estimatePriceAdjustAmountTax + estimateStepRebateAmountTax) AS grossProfitEstimateSubsidy
					, netSalesMoneyTax - netSalesCostTax - overseasTax + otherIncomeMoneyCnyTax - costAdjustMoneyCnyTax + (grossSubsidyAmountTax + returnSubsidyAmountTax + priceAdjustAmountTax + stepRebateAmountTax + returnDiscountsAmountTax + nationalInspectionAmountTax + costAdjustmentAmountTax + vendorDiscountsAmountTax + incomingDifferenceAmountTax + generalTradeAmountTax + inventorySubsidyAmountTax + memberReturnAmountTax + otdStationOutAmountTax + otdStationInAmountTax + brandPromotionAmountTax) AS grossProfitSubsidy
					, originNetSalesMoney - netSalesCost - freightCostCny - settlServiceAndTax - overseasTax - marketCost - technicalCost - adminCost - departmentCost - businessSupportCost - customerServiceCost + otherIncomeMoneyCny - costAdjustMoneyCny AS netProfit
					, originNetSalesMoney - netSalesCost - freightCostCny - settlServiceAndTax - overseasTax - marketCost - technicalCost - adminCost - departmentCost - businessSupportCost - customerServiceCost + otherIncomeMoneyCny - costAdjustMoneyCny + (estimateSalesRebateAmount + estimateGrossSubsidyAmount + estimateReturnSubsidyAmount + estimatePriceAdjustAmount + estimateStepRebateAmount) AS netProfitEstimateSubsidy
					, originNetSalesMoney - netSalesCost - freightCostCny - settlServiceAndTax - overseasTax - marketCost - technicalCost - adminCost - departmentCost - businessSupportCost - customerServiceCost + otherIncomeMoneyCny - costAdjustMoneyCny + (grossSubsidyAmount + returnSubsidyAmount + priceAdjustAmount + stepRebateAmount + returnDiscountsAmount + nationalInspectionAmount + costAdjustmentAmount + vendorDiscountsAmount + incomingDifferenceAmount + generalTradeAmount + inventorySubsidyAmount + memberReturnAmount + otdStationOutAmount + otdStationInAmount + brandPromotionAmount) AS netProfitSubsidy
					, estimateSalesRebateAmount + estimateGrossSubsidyAmount + estimateReturnSubsidyAmount + estimatePriceAdjustAmount + estimateStepRebateAmount AS estimateRebateSubsidy
					, estimateSalesRebateAmountTax + estimateGrossSubsidyAmountTax + estimateReturnSubsidyAmountTax + estimatePriceAdjustAmountTax + estimateStepRebateAmountTax AS estimateRebateSubsidyTax
					, grossSubsidyAmount + returnSubsidyAmount + priceAdjustAmount + stepRebateAmount + returnDiscountsAmount + nationalInspectionAmount + costAdjustmentAmount + vendorDiscountsAmount + incomingDifferenceAmount + generalTradeAmount + inventorySubsidyAmount + memberReturnAmount + otdStationOutAmount + otdStationInAmount + brandPromotionAmount AS rebateSubsidy
					, grossSubsidyAmountTax + returnSubsidyAmountTax + priceAdjustAmountTax + stepRebateAmountTax + returnDiscountsAmountTax + nationalInspectionAmountTax + costAdjustmentAmountTax + vendorDiscountsAmountTax + incomingDifferenceAmountTax + generalTradeAmountTax + inventorySubsidyAmountTax + memberReturnAmountTax + otdStationOutAmountTax + otdStationInAmountTax + brandPromotionAmountTax AS rebateSubsidyTax
					, estimateSalesRebateAmount, estimateSalesRebateAmountTax, estimateGrossSubsidyAmount, estimateGrossSubsidyAmountTax, estimateReturnSubsidyAmount
					, estimateReturnSubsidyAmountTax, estimatePriceAdjustAmount, estimatePriceAdjustAmountTax, estimateStepRebateAmount, estimateStepRebateAmountTax
					, SalesRebateAmount, SalesRebateAmountTax, GrossSubsidyAmount, GrossSubsidyAmountTax, ReturnSubsidyAmount
					, ReturnSubsidyAmountTax, PriceAdjustAmount, PriceAdjustAmountTax, StepRebateAmount, StepRebateAmountTax
					, returnDiscountsAmountTax, returnDiscountsAmount, nationalInspectionAmountTax, nationalInspectionAmount, costAdjustmentAmountTax
					, costAdjustmentAmount, vendorDiscountsAmountTax, vendorDiscountsAmount, incomingDifferenceAmountTax, incomingDifferenceAmount
					, generalTradeAmountTax, generalTradeAmount, inventorySubsidyAmountTax, inventorySubsidyAmount, memberReturnAmountTax
					, memberReturnAmount, otdStationOutAmountTax, otdStationOutAmount, otdStationInAmountTax, otdStationInAmount
					, brandPromotionAmountTax, brandPromotionAmount
				FROM (
					SELECT dt, CAST(hour AS int) AS hour, salesOrderTotalQty, salesQty, netSalesTotalMoneyTax
						, salesMoney, salesMoneyTax, netSalesMoney, salesMoneyTax - busiDiscountAmountTax AS netSalesMoneyTax
						, originNetSalesMoney, tailnetSalesTotalMoneyTax, salesCost, salesCostTax
						, salesCost - vendorDiscountAmount AS netSalesCost, salesCostTax - vendorDiscountAmountTax AS netSalesCostTax
						, busiDiscountAmountTax, discountAmount, otherTotalDiscountAmount, vendorDiscountAmount, vendorDiscountAmountTax
						, overseasTax, freightIncomeCny
						, freightCostCny + salesOrderTotalQty * svip_rj_rate * svip_order_rate * 9.43 / 1.06 AS freightCostCny
						, vcardAmount, addTaxAmount + stampsAmount + settlServiceAmount AS settlServiceAndTax, marketCost
						, technicalCost, adminCost, departmentCost, businessSupportCost, customerServiceCost
						, 0 AS costAdjustMoneyCny, 0 AS costAdjustMoneyCnyTax, 0 AS otherIncomeMoneyCny, 0 AS otherIncomeMoneyCnyTax, estimateSalesRebateAmount AS estimateSalesRebateAmount
						, estimateSalesRebateAmountTax AS estimateSalesRebateAmountTax, estimateGrossSubsidyAmount AS estimateGrossSubsidyAmount, estimateGrossSubsidyAmountTax AS estimateGrossSubsidyAmountTax, estimateReturnSubsidyAmount AS estimateReturnSubsidyAmount, estimateReturnSubsidyAmountTax AS estimateReturnSubsidyAmountTax
						, estimatePriceAdjustAmount AS estimatePriceAdjustAmount, estimatePriceAdjustAmountTax AS estimatePriceAdjustAmountTax, estimateStepRebateAmount AS estimateStepRebateAmount, estimateStepRebateAmountTax AS estimateStepRebateAmountTax, 0 AS SalesRebateAmount
						, 0 AS SalesRebateAmountTax, 0 AS GrossSubsidyAmount, 0 AS GrossSubsidyAmountTax, 0 AS ReturnSubsidyAmount, 0 AS ReturnSubsidyAmountTax
						, 0 AS PriceAdjustAmount, 0 AS PriceAdjustAmountTax, 0 AS StepRebateAmount, 0 AS StepRebateAmountTax, 0 AS returnDiscountsAmountTax
						, 0 AS returnDiscountsAmount, 0 AS nationalInspectionAmountTax, 0 AS nationalInspectionAmount, 0 AS costAdjustmentAmountTax, 0 AS costAdjustmentAmount
						, 0 AS vendorDiscountsAmountTax, 0 AS vendorDiscountsAmount, 0 AS incomingDifferenceAmountTax, 0 AS incomingDifferenceAmount, 0 AS generalTradeAmountTax
						, 0 AS generalTradeAmount, 0 AS inventorySubsidyAmountTax, 0 AS inventorySubsidyAmount, 0 AS memberReturnAmountTax, 0 AS memberReturnAmount
						, otd_brand.otd_station_out_amount_tax AS otdStationOutAmountTax, otd_brand.otd_station_out_amount AS otdStationOutAmount, otd_brand.otd_station_in_amount_tax AS otdStationInAmountTax, otd_brand.otd_station_in_amount AS otdStationInAmount, otd_brand.brand_promotion_amount_tax AS brandPromotionAmountTax
						, otd_brand.brand_promotion_amount AS brandPromotionAmount
					FROM (
						SELECT dt, hour, COUNT(DISTINCT CASE 
								WHEN order_ext_flags <> '2' THEN order_sn
							END) AS salesOrderTotalQty
							, SUM(sales_qty) AS salesQty, SUM(net_sales_total_money_tax) AS netSalesTotalMoneyTax
							, SUM(sales_money * rj_rate) AS salesMoney
							, SUM(sales_money_tax * rj_rate) AS salesMoneyTax
							, SUM(net_sales_money * rj_rate) AS netSalesMoney
							, SUM(origin_net_sales_money * rj_rate) AS originNetSalesMoney
							, SUM(tailnet_sales_total_money_tax) AS tailnetSalesTotalMoneyTax
							, SUM(sales_cost * rj_rate) AS salesCost
							, SUM(sales_cost_tax * rj_rate) AS salesCostTax
							, SUM(busi_discount_amount_tax * rj_rate) AS busiDiscountAmountTax
							, SUM(discount_amount * rj_rate) AS discountAmount
							, SUM(other_total_discount_amount * rj_rate) AS otherTotalDiscountAmount
							, SUM(vendor_discount_amount * rj_rate) AS vendordiscountAmount
							, SUM(vendor_discount_amount_tax * rj_rate) AS vendordiscountAmountTax
							, SUM(add_tax_amount * rj_rate) AS addTaxAmount
							, SUM(stamps_amount * rj_rate) AS stampsAmount
							, SUM(settl_service_amount) AS settlServiceAmount, SUM(overseas_tax) AS overseasTax
							, SUM(freight_income_cny * rj_rate) AS freightIncomeCny
							, SUM(freight_cost_cny) AS freightCostCny
							, SUM(vcard_amount * rj_rate) AS vcardAmount
							, SUM(market_cost) AS marketCost, SUM(technical_cost) AS technicalCost
							, SUM(admin_cost) AS adminCost, SUM(department_cost) AS departmentCost
							, SUM(business_support_cost) AS businessSupportCost, SUM(customer_service_cost) AS customerServiceCost
							, SUM(comprehensive_gross_profit * rj_rate) AS comprehensiveGrossProfit
							, MIN(svip_rj_rate) AS svip_rj_rate, MIN(svip_order_rate) AS svip_order_rate
							, SUM(estimateSalesRebateAmount) AS estimateSalesRebateAmount, SUM(estimateSalesRebateAmountTax) AS estimateSalesRebateAmountTax
							, SUM(estimateGrossSubsidyAmount) AS estimateGrossSubsidyAmount, SUM(estimateGrossSubsidyAmountTax) AS estimateGrossSubsidyAmountTax
							, SUM(estimateReturnSubsidyAmount) AS estimateReturnSubsidyAmount, SUM(estimateReturnSubsidyAmountTax) AS estimateReturnSubsidyAmountTax
							, SUM(estimatePriceAdjustAmount) AS estimatePriceAdjustAmount, SUM(estimatePriceAdjustAmountTax) AS estimatePriceAdjustAmountTax
							, SUM(estimateStepRebateAmount) AS estimateStepRebateAmount, SUM(estimateStepRebateAmountTax) AS estimateStepRebateAmountTax
						FROM (
							SELECT dt, order_sn, item_code, sales_schedule_id, purchase_schedule_id
								, vsku_id, mer_item_no, vendor_code, MIN(hour) AS hour
								, MIN(order_ext_flags) AS order_ext_flags
								, 1 - MIN(COALESCE(b.rj_rate, c.rj_rate, d.rj_rate)) AS rj_rate
								, MIN(e.rj_rate) AS svip_rj_rate, MIN(e.order_rate) AS svip_order_rate
								, SUM(CASE 
									WHEN order_ext_flags = '2' THEN 0
									ELSE sales_qty
								END) AS sales_qty, SUM(CASE 
									WHEN order_ext_flags = '2' THEN 0
									ELSE sales_money
								END) AS sales_money
								, SUM(CASE 
									WHEN order_ext_flags = '2' THEN 0
									ELSE sales_money_tax
								END) AS sales_money_tax, SUM(CASE 
									WHEN order_ext_flags = '2' THEN 0
									ELSE net_sales_money
								END) AS net_sales_money
								, SUM(CASE 
									WHEN order_ext_flags = '2' THEN 0
									ELSE origin_net_sales_money
								END) AS origin_net_sales_money, SUM(CASE 
									WHEN order_ext_flags = '2' THEN pay_amount
									ELSE 0
								END) AS tailnet_sales_total_money_tax
								, SUM(CASE 
									WHEN order_ext_flags IN ('1', '2') THEN COALESCE(pay_amount, 0)
									ELSE COALESCE(sales_Money_Tax, 0) + COALESCE(discount_amount_tax, 0) + COALESCE(other_total_discount_amount_tax, 0) + COALESCE(freight_income_cny_tax, 0) - COALESCE(vcard_amount_tax, 0)
								END) AS net_sales_total_money_tax, SUM(CASE 
									WHEN order_ext_flags = '2' THEN 0
									ELSE sales_cost
								END) AS sales_cost
								, SUM(CASE 
									WHEN order_ext_flags = '2' THEN 0
									ELSE sales_cost_tax
								END) AS sales_cost_tax, SUM(CASE 
									WHEN order_ext_flags = '2' THEN 0
									ELSE busi_discount_amount_tax_tf
								END) AS busi_discount_amount_tax
								, SUM(CASE 
									WHEN order_ext_flags = '2' THEN 0
									ELSE discount_amount_tf
								END) AS discount_amount, SUM(CASE 
									WHEN order_ext_flags = '2' THEN 0
									ELSE other_total_discount_amount_tf
								END) AS other_total_discount_amount
								, SUM(CASE 
									WHEN order_ext_flags = '2' THEN 0
									ELSE vendor_discount_amount_tf
								END) AS vendor_discount_amount, SUM(CASE 
									WHEN order_ext_flags = '2' THEN 0
									ELSE vendor_discount_amount_tax_tf
								END) AS vendor_discount_amount_tax
								, SUM(CASE 
									WHEN order_ext_flags = '2' THEN 0
									ELSE add_tax_amount
								END) AS add_tax_amount, SUM(CASE 
									WHEN order_ext_flags = '2' THEN 0
									ELSE stamps_amount
								END) AS stamps_amount
								, SUM(CASE 
									WHEN order_ext_flags = '2' THEN 0
									ELSE settl_service_amount
								END) AS settl_service_amount, SUM(CASE 
									WHEN order_ext_flags = '2' THEN 0
									ELSE overseas_tax
								END) AS overseas_tax
								, SUM(CASE 
									WHEN order_ext_flags = '2' THEN 0
									ELSE freight_income_cny
								END) AS freight_Income_Cny, SUM(CASE 
									WHEN order_ext_flags = '2' THEN 0
									ELSE freight_cost_cny
								END) AS freight_cost_cny
								, SUM(CASE 
									WHEN order_ext_flags = '2' THEN 0
									ELSE vcard_amount
								END) AS vcard_amount, SUM(CASE 
									WHEN order_ext_flags = '2' THEN 0
									ELSE market_cost
								END) AS market_cost
								, SUM(CASE 
									WHEN order_ext_flags = '2' THEN 0
									ELSE technical_cost
								END) AS technical_cost, SUM(CASE 
									WHEN order_ext_flags = '2' THEN 0
									ELSE admin_cost
								END) AS admin_cost
								, SUM(CASE 
									WHEN order_ext_flags = '2' THEN 0
									ELSE department_cost
								END) AS department_cost, SUM(CASE 
									WHEN order_ext_flags = '2' THEN 0
									ELSE business_support_cost
								END) AS business_support_cost
								, SUM(CASE 
									WHEN order_ext_flags = '2' THEN 0
									ELSE customer_service_cost
								END) AS customer_service_cost, SUM(CASE 
									WHEN order_ext_flags = '2' THEN 0
									ELSE comprehensive_gross_profit
								END) AS comprehensive_gross_profit
								, SUM(CASE 
									WHEN order_ext_flags = '2' THEN 0
									ELSE estimateSalesRebateAmount
								END) AS estimateSalesRebateAmount, SUM(CASE 
									WHEN order_ext_flags = '2' THEN 0
									ELSE estimateSalesRebateAmountTax
								END) AS estimateSalesRebateAmountTax
								, SUM(CASE 
									WHEN order_ext_flags = '2' THEN 0
									ELSE estimateGrossSubsidyAmount
								END) AS estimateGrossSubsidyAmount, SUM(CASE 
									WHEN order_ext_flags = '2' THEN 0
									ELSE estimateGrossSubsidyAmountTax
								END) AS estimateGrossSubsidyAmountTax
								, SUM(CASE 
									WHEN order_ext_flags = '2' THEN 0
									ELSE estimateReturnSubsidyAmount
								END) AS estimateReturnSubsidyAmount, SUM(CASE 
									WHEN order_ext_flags = '2' THEN 0
									ELSE estimateReturnSubsidyAmountTax
								END) AS estimateReturnSubsidyAmountTax
								, SUM(CASE 
									WHEN order_ext_flags = '2' THEN 0
									ELSE estimatePriceAdjustAmount
								END) AS estimatePriceAdjustAmount, SUM(CASE 
									WHEN order_ext_flags = '2' THEN 0
									ELSE estimatePriceAdjustAmountTax
								END) AS estimatePriceAdjustAmountTax
								, SUM(CASE 
									WHEN order_ext_flags = '2' THEN 0
									ELSE estimateStepRebateAmount
								END) AS estimateStepRebateAmount, SUM(CASE 
									WHEN order_ext_flags = '2' THEN 0
									ELSE estimateStepRebateAmountTax
								END) AS estimateStepRebateAmountTax
							FROM (
								SELECT *
									, CASE 
										WHEN pur_sche_dept_oa_code = '' THEN 'TS09999'
										ELSE pur_sche_dept_oa_code
									END AS responsible_department_code
									, CASE 
										WHEN pur_sche_dept_name_l2 = '' THEN '其他部门'
										ELSE pur_sche_dept_name_l2
									END AS responsible_department_name, -busi_discount_amount_tax AS busi_discount_amount_tax_tf, -discount_amount AS discount_amount_tf, -other_total_discount_amount AS other_total_discount_amount_tf, -(COALESCE(vendor_discount_amount, 0) + COALESCE(other_vendor_discount_amount, 0)) AS vendor_discount_amount_tf
									, -(COALESCE(vendor_discount_amount_tax, 0) + COALESCE(other_vendor_discount_amount_tax, 0)) AS vendor_discount_amount_tax_tf, 0 AS estimateSalesRebateAmount, 0 AS estimateSalesRebateAmountTax, 0 AS estimateGrossSubsidyAmount, 0 AS estimateGrossSubsidyAmountTax
									, 0 AS estimateReturnSubsidyAmount, 0 AS estimateReturnSubsidyAmountTax, 0 AS estimatePriceAdjustAmount, 0 AS estimatePriceAdjustAmountTax, 0 AS estimateStepRebateAmount
									, 0 AS estimateStepRebateAmountTax
								FROM vipvis.sales_order_detail_real_time
								WHERE dt BETWEEN format_datetime(date_add('day', -1, current_date), 'yyyyMMdd') AND format_datetime(current_date, 'yyyyMMdd')
									AND 1 = 1
									AND dt = format_datetime(current_date, 'yyyyMMdd')
									AND 1 = 1
									AND 1 = 1
									AND 1 = 1
							) origin_info
								LEFT JOIN (
									SELECT rj_rate, brand_code
									FROM vipvis.vis_report_fx_sales_related_rj_rate_brand
								) b
								ON origin_info.brand_code = b.brand_code
								LEFT JOIN (
									SELECT rj_rate, responsible_department_code
									FROM vipvis.vis_report_fx_sales_related_rj_rate_department
								) c
								ON origin_info.responsible_department_code = c.responsible_department_code
								CROSS JOIN (
									SELECT rj_rate
									FROM vipvis.vis_report_fx_sales_related_rj_rate_all
								) d
								CROSS JOIN (
									SELECT rj_rate, order_rate
									FROM vipvis.vis_report_fx_sales_related_rj_rate_svip
								) e
							GROUP BY dt, order_sn, item_code, sales_schedule_id, purchase_schedule_id, vsku_id, mer_item_no, vendor_code
						) order_item
						WHERE order_item.sales_qty > 0
							AND order_item.sales_money_tax > 0
						GROUP BY dt, hour
					) order_agg
						CROSS JOIN (
							SELECT SUM(otd_station_out_amount_tax) / 24 AS otd_station_out_amount_tax
								, SUM(otd_station_out_amount) / 24 AS otd_station_out_amount
								, SUM(otd_station_in_amount_tax) / 24 AS otd_station_in_amount_tax
								, SUM(otd_station_in_amount) / 24 AS otd_station_in_amount
								, SUM(brand_promotion_amount_tax) / 24 AS brand_promotion_amount_tax
								, SUM(brand_promotion_amount) / 24 AS brand_promotion_amount
							FROM temp_vis.ebs_other_income_rebate_day_info
							WHERE month = format_datetime(current_date, 'yyyyMM')
						) otd_brand
					UNION ALL
					SELECT dt, NULL AS hour, 0 AS salesOrderTotalQty, salesQty, netSalesTotalMoneyTax
						, salesMoney, salesMoneyTax, netSalesMoney, salesMoneyTax - busiDiscountAmountTax AS netSalesMoneyTax
						, originNetSalesMoney, 0 AS tailnetSalesTotalMoneyTax, salesCost, salesCostTax, netSalesCost
						, salesCostTax - vendorDiscountAmountTax AS netSalesCostTax, busiDiscountAmountTax, discountAmount
						, otherTotalDiscountAmount, vendorDiscountAmount, vendorDiscountAmountTax, overseasTax, freightIncomeCny
						, freightCostCny, vcardAmount, addTaxAmount + stampsAmount + settlServiceAmount AS settlServiceAndTax
						, marketCost, technicalCost, adminCost, departmentCost, businessSupportCost
						, customerServiceCost, costAdjustMoneyCny, costAdjustMoneyCnyTax, otherIncomeMoneyCny, otherIncomeMoneyCnyTax
						, estimateSalesRebateAmount, estimateSalesRebateAmountTax, estimateGrossSubsidyAmount, estimateGrossSubsidyAmountTax, estimateReturnSubsidyAmount
						, estimateReturnSubsidyAmountTax, estimatePriceAdjustAmount, estimatePriceAdjustAmountTax, estimateStepRebateAmount, estimateStepRebateAmountTax
						, salesRebateAmount, salesRebateAmountTax, grossSubsidyAmount, grossSubsidyAmountTax, returnSubsidyAmount
						, returnSubsidyAmountTax, priceAdjustAmount, priceAdjustAmountTax, stepRebateAmount, stepRebateAmountTax
						, returnDiscountsAmountTax, returnDiscountsAmount, nationalInspectionAmountTax, nationalInspectionAmount, costAdjustmentAmountTax
						, costAdjustmentAmount, vendorDiscountsAmountTax, vendorDiscountsAmount, incomingDifferenceAmountTax, incomingDifferenceAmount
						, generalTradeAmountTax, generalTradeAmount, inventorySubsidyAmountTax, inventorySubsidyAmount, memberReturnAmountTax
						, memberReturnAmount, otdStationOutAmountTax, otdStationOutAmount, otdStationInAmountTax, otdStationInAmount
						, brandPromotionAmountTax, brandPromotionAmount
					FROM (
						SELECT dt, SUM(CASE 
								WHEN order_ext_flags = '2' THEN 0
								ELSE sales_qty
							END) AS salesQty, SUM(CASE 
								WHEN order_ext_flags = '2' THEN 0
								ELSE net_sales_total_money_tax
							END) AS netSalesTotalMoneyTax
							, SUM(CASE 
								WHEN order_ext_flags = '2' THEN 0
								ELSE sales_money
							END) AS salesMoney, SUM(CASE 
								WHEN order_ext_flags = '2' THEN 0
								ELSE sales_money_tax
							END) AS salesMoneyTax
							, SUM(CASE 
								WHEN order_ext_flags = '2' THEN 0
								ELSE net_sales_money
							END) AS netSalesMoney, SUM(CASE 
								WHEN order_ext_flags = '2' THEN 0
								ELSE COALESCE(net_sales_money_tax, 0) + COALESCE(other_total_discount_amount_tax, 0)
							END) AS netSalesMoneyTax
							, SUM(CASE 
								WHEN order_ext_flags = '2' THEN 0
								ELSE origin_net_sales_money
							END) AS originNetSalesMoney, SUM(CASE 
								WHEN order_ext_flags = '2' THEN 0
								ELSE sales_cost
							END) AS salesCost
							, SUM(CASE 
								WHEN order_ext_flags = '2' THEN 0
								ELSE sales_cost_tax
							END) AS salesCostTax, SUM(CASE 
								WHEN order_ext_flags = '2' THEN 0
								ELSE net_sales_cost
							END) AS netSalesCost
							, SUM(CASE 
								WHEN order_ext_flags = '2' THEN 0
								ELSE net_sales_cost_tax
							END) AS netSalesCostTax, -SUM(CASE 
								WHEN order_ext_flags = '2' THEN 0
								ELSE COALESCE(discount_amount_tax, 0) + COALESCE(other_total_discount_amount_tax, 0)
							END) AS busiDiscountAmountTax, -SUM(CASE 
								WHEN order_ext_flags = '2' THEN 0
								ELSE discount_amount
							END) AS discountAmount
							, -SUM(CASE 
								WHEN order_ext_flags = '2' THEN 0
								ELSE other_total_discount_amount
							END) AS otherTotalDiscountAmount, -SUM(CASE 
								WHEN order_ext_flags = '2' THEN 0
								ELSE COALESCE(vendor_discount_amount, 0) + COALESCE(other_vendor_discount_amount, 0)
							END) AS vendorDiscountAmount, -SUM(CASE 
								WHEN order_ext_flags = '2' THEN 0
								ELSE COALESCE(vendor_discount_amount_tax, 0) + COALESCE(other_vendor_discount_amount_tax, 0)
							END) AS vendorDiscountAmountTax, SUM(CASE 
								WHEN order_ext_flags = '2' THEN 0
								ELSE overseas_tax
							END) AS overseasTax
							, SUM(CASE 
								WHEN order_ext_flags = '2' THEN 0
								ELSE freight_Income_Cny
							END) AS freightIncomeCny, SUM(CASE 
								WHEN order_ext_flags = '2' THEN 0
								ELSE freight_cost_cny
							END) AS freightCostCny
							, SUM(CASE 
								WHEN order_ext_flags = '2' THEN 0
								ELSE vcard_Amount
							END) AS vcardAmount, SUM(CASE 
								WHEN order_ext_flags = '2' THEN 0
								ELSE add_tax_amount
							END) AS addTaxAmount
							, SUM(CASE 
								WHEN order_ext_flags = '2' THEN 0
								ELSE stamps_amount
							END) AS stampsAmount, SUM(CASE 
								WHEN order_ext_flags = '2' THEN 0
								ELSE settl_service_amount
							END) AS settlServiceAmount
							, SUM(CASE 
								WHEN order_ext_flags = '2' THEN 0
								ELSE market_cost
							END) AS marketCost, SUM(CASE 
								WHEN order_ext_flags = '2' THEN 0
								ELSE technical_cost
							END) AS technicalCost
							, SUM(CASE 
								WHEN order_ext_flags = '2' THEN 0
								ELSE admin_cost
							END) AS adminCost, SUM(CASE 
								WHEN order_ext_flags = '2' THEN 0
								ELSE department_cost
							END) AS departmentCost
							, SUM(CASE 
								WHEN order_ext_flags = '2' THEN 0
								ELSE business_support_cost
							END) AS businessSupportCost, SUM(CASE 
								WHEN order_ext_flags = '2' THEN 0
								ELSE customer_service_cost
							END) AS customerServiceCost
							, SUM(CASE 
								WHEN order_ext_flags = '2' THEN 0
								ELSE cost_adjust_money_cny
							END) AS costAdjustMoneyCny, SUM(CASE 
								WHEN order_ext_flags = '2' THEN 0
								ELSE cost_adjust_money_cny_tax
							END) AS costAdjustMoneyCnyTax
							, SUM(CASE 
								WHEN order_ext_flags = '2' THEN 0
								ELSE other_income_money_cny
							END) AS otherIncomeMoneyCny, SUM(CASE 
								WHEN order_ext_flags = '2' THEN 0
								ELSE other_income_money_cny_tax
							END) AS otherIncomeMoneyCnyTax
							, SUM(CASE 
								WHEN order_ext_flags = '2' THEN 0
								ELSE 0
							END) AS estimateSalesRebateAmount, SUM(CASE 
								WHEN order_ext_flags = '2' THEN 0
								ELSE 0
							END) AS estimateSalesRebateAmountTax
							, SUM(CASE 
								WHEN order_ext_flags = '2' THEN 0
								ELSE 0
							END) AS estimateGrossSubsidyAmount, SUM(CASE 
								WHEN order_ext_flags = '2' THEN 0
								ELSE 0
							END) AS estimateGrossSubsidyAmountTax
							, SUM(CASE 
								WHEN order_ext_flags = '2' THEN 0
								ELSE 0
							END) AS estimateReturnSubsidyAmount, SUM(CASE 
								WHEN order_ext_flags = '2' THEN 0
								ELSE 0
							END) AS estimateReturnSubsidyAmountTax
							, SUM(CASE 
								WHEN order_ext_flags = '2' THEN 0
								ELSE 0
							END) AS estimatePriceAdjustAmount, SUM(CASE 
								WHEN order_ext_flags = '2' THEN 0
								ELSE 0
							END) AS estimatePriceAdjustAmountTax
							, SUM(CASE 
								WHEN order_ext_flags = '2' THEN 0
								ELSE 0
							END) AS estimateStepRebateAmount, SUM(CASE 
								WHEN order_ext_flags = '2' THEN 0
								ELSE 0
							END) AS estimateStepRebateAmountTax
							, SUM(CASE 
								WHEN order_ext_flags = '2' THEN 0
								ELSE 0
							END) AS salesRebateAmount, SUM(CASE 
								WHEN order_ext_flags = '2' THEN 0
								ELSE 0
							END) AS salesRebateAmountTax
							, SUM(CASE 
								WHEN order_ext_flags = '2' THEN 0
								ELSE 0
							END) AS grossSubsidyAmount, SUM(CASE 
								WHEN order_ext_flags = '2' THEN 0
								ELSE 0
							END) AS grossSubsidyAmountTax
							, SUM(CASE 
								WHEN order_ext_flags = '2' THEN 0
								ELSE 0
							END) AS returnSubsidyAmount, SUM(CASE 
								WHEN order_ext_flags = '2' THEN 0
								ELSE 0
							END) AS returnSubsidyAmountTax
							, SUM(CASE 
								WHEN order_ext_flags = '2' THEN 0
								ELSE 0
							END) AS priceAdjustAmount, SUM(CASE 
								WHEN order_ext_flags = '2' THEN 0
								ELSE 0
							END) AS priceAdjustAmountTax
							, SUM(CASE 
								WHEN order_ext_flags = '2' THEN 0
								ELSE 0
							END) AS stepRebateAmount, SUM(CASE 
								WHEN order_ext_flags = '2' THEN 0
								ELSE 0
							END) AS stepRebateAmountTax
							, SUM(CASE 
								WHEN order_ext_flags = '2' THEN 0
								ELSE 0
							END) AS returnDiscountsAmountTax, SUM(CASE 
								WHEN order_ext_flags = '2' THEN 0
								ELSE 0
							END) AS returnDiscountsAmount
							, SUM(CASE 
								WHEN order_ext_flags = '2' THEN 0
								ELSE 0
							END) AS nationalInspectionAmountTax, SUM(CASE 
								WHEN order_ext_flags = '2' THEN 0
								ELSE 0
							END) AS nationalInspectionAmount
							, SUM(CASE 
								WHEN order_ext_flags = '2' THEN 0
								ELSE 0
							END) AS costAdjustmentAmountTax, SUM(CASE 
								WHEN order_ext_flags = '2' THEN 0
								ELSE 0
							END) AS costAdjustmentAmount
							, SUM(CASE 
								WHEN order_ext_flags = '2' THEN 0
								ELSE 0
							END) AS vendorDiscountsAmountTax, SUM(CASE 
								WHEN order_ext_flags = '2' THEN 0
								ELSE 0
							END) AS vendorDiscountsAmount
							, SUM(CASE 
								WHEN order_ext_flags = '2' THEN 0
								ELSE 0
							END) AS incomingDifferenceAmountTax, SUM(CASE 
								WHEN order_ext_flags = '2' THEN 0
								ELSE 0
							END) AS incomingDifferenceAmount
							, SUM(CASE 
								WHEN order_ext_flags = '2' THEN 0
								ELSE 0
							END) AS generalTradeAmountTax, SUM(CASE 
								WHEN order_ext_flags = '2' THEN 0
								ELSE 0
							END) AS generalTradeAmount
							, SUM(CASE 
								WHEN order_ext_flags = '2' THEN 0
								ELSE 0
							END) AS inventorySubsidyAmountTax, SUM(CASE 
								WHEN order_ext_flags = '2' THEN 0
								ELSE 0
							END) AS inventorySubsidyAmount
							, SUM(CASE 
								WHEN order_ext_flags = '2' THEN 0
								ELSE 0
							END) AS memberReturnAmountTax, SUM(CASE 
								WHEN order_ext_flags = '2' THEN 0
								ELSE 0
							END) AS memberReturnAmount
							, SUM(CASE 
								WHEN order_ext_flags = '2' THEN 0
								ELSE 0
							END) AS otdStationOutAmountTax, SUM(CASE 
								WHEN order_ext_flags = '2' THEN 0
								ELSE 0
							END) AS otdStationOutAmount
							, SUM(CASE 
								WHEN order_ext_flags = '2' THEN 0
								ELSE 0
							END) AS otdStationInAmountTax, SUM(CASE 
								WHEN order_ext_flags = '2' THEN 0
								ELSE 0
							END) AS otdStationInAmount
							, SUM(CASE 
								WHEN order_ext_flags = '2' THEN 0
								ELSE 0
							END) AS brandPromotionAmountTax, SUM(CASE 
								WHEN order_ext_flags = '2' THEN 0
								ELSE 0
							END) AS brandPromotionAmount
						FROM vipvis.sales_info_outbound_depat_brand_schedule_day origin_info
						WHERE dt <> format_datetime(date_add('day', -1, current_date), 'yyyyMMdd')
							AND 1 = 1
							AND dt = format_datetime(current_date, 'yyyyMMdd')
							AND 1 = 1
							AND 1 = 1
							AND 1 = 1
						GROUP BY dt
					) his_info
				) final_info
			) tmp
			
		) rw_tmp
	)
SELECT *
FROM tmp tmp
WHERE RW BETWEEN 1 AND 3000