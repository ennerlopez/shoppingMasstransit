using System;
using System.Linq.Expressions;
using Automatonymous;
using Automatonymous.UserTypes;
using NHibernate.Mapping.ByCode;
using CartTracking;
using MassTransit.EntityFrameworkIntegration;

namespace TrackingService
{
   

   
    public static class AutomatonymousNHibernateExtensions
    {
        public static void StateProperty<T, TMachine>(this IClassMapper<T> mapper,
            Expression<Func<T, State>> stateExpression)
            where T : class
            where TMachine : StateMachine
        {
            mapper.Property(stateExpression, x =>
            {
                x.Type<AutomatonymousStateUserType<TMachine>>();
                x.NotNullable(true);
                x.Length(80);
            });
        }
    }

    public class ShoppingCartMap :
       MassTransit.NHibernateIntegration.SagaClassMapping<ShoppingCart>
    {
        public ShoppingCartMap()
        {
           
           //this.StateProperty<ShoppingCart, ShoppingCartStateMachine>(x => x.CurrentState);
            Property(x => x.CurrentState);
                //.HasMaxLength(64);

            Property(x => x.Created);
            Property(x => x.Updated);

            Property(x => x.UserName);
            //    .HasMaxLength(256);

            Property(x => x.ExpirationId);
            Property(x => x.OrderId);
        }
    }

}