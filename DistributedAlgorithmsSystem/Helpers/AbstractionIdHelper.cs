namespace DistributedAlgorithmsSystem.Helpers;

public static class AbstractionIdHelper {
     private static string RemoveBrackets(this string abstractionId) {
         var indexOf = abstractionId.IndexOf('[');
         return indexOf == -1 ? abstractionId : abstractionId[..indexOf];
     }

     public static string GetTopic(this string abstractionId) {
         var indexOf = abstractionId.IndexOf('[');
         return abstractionId[(indexOf + 1)..^1];
     }

     public static string GetEndAbstraction(this string abstractionId) {
         var indexOf = abstractionId.LastIndexOf('.');
         return (indexOf == -1 ? abstractionId : abstractionId[(indexOf + 1)..]).RemoveBrackets();
     }

     public static string GetAbstraction(this string abstractionId,string lastAbstraction) {
         while (abstractionId.GetEndAbstraction() != lastAbstraction)
             abstractionId = abstractionId.RemoveEndAbstraction();
         return abstractionId;
     }

     public static string RemoveEndAbstraction(this string abstractionId) {
         var indexOf = abstractionId.LastIndexOf('.');
         return abstractionId[..indexOf];
     }
 }