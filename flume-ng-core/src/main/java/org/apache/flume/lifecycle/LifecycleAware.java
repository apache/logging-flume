package org.apache.flume.lifecycle;

import org.apache.flume.Context;

/**
 * <p>
 * An interface implemented by any class that has a defined, stateful,
 * lifecycle.
 * </p>
 * <p>
 * Implementations of {@link LifecycleAware} conform to a standard method of
 * starting, stopping, and reporting their current state. Additionally, this
 * interface creates a standard method of communicating failure to perform a
 * lifecycle operation to the caller (i.e. via {@link LifecycleException}). It
 * is never considered valid to call {@link #start(Context)} or
 * {@link #stop(Context)} more than once or to call them in the wrong order.
 * While this is not strictly enforced, it may be in the future.
 * </p>
 * <p>
 * Example services may include Flume nodes and the master, but also lower level
 * components that can be controlled in a similar manner.
 * </p>
 * <p>
 * Example usage
 * </p>
 * <code>
 *  public class MyService implements LifecycleAware {
 * 
 *    private LifecycleState lifecycleState;
 * 
 *    public MyService() {
 *      lifecycleState = LifecycleState.IDLE;
 *    }
 * 
 *    @Override
 *    public void start(Context context) throws LifecycleException,
 *      InterruptedException {
 * 
 *      ...your code does something.
 * 
 *      lifecycleState = LifecycleState.START;
 *    }
 * 
 *    @Override
 *    public void stop(Context context) throws LifecycleException,
 *      InterruptedException {
 * 
 *      try {
 *        ...you stop services here.
 *      } catch (SomethingException) {
 *        lifecycleState = LifecycleState.ERROR;
 *      }
 * 
 *      lifecycleState = LifecycleState.STOP;
 *    }
 * 
 *    @Override
 *    public LifecycleState getLifecycleState() {
 *      return lifecycleState;
 *    }
 * 
 *  }
 * </code>
 */
public interface LifecycleAware {

  /**
   * <p>
   * Starts a service or component.
   * </p>
   * <p>
   * Implementations should determine the result of any start logic and effect
   * the return value of {@link #getLifecycleState()} accordingly.
   * </p>
   * 
   * @param context
   * @throws LifecycleException
   * @throws InterruptedException
   */
  public void start(Context context);

  /**
   * <p>
   * Stops a service or component.
   * </p>
   * <p>
   * Implementations should determine the result of any stop logic and effect
   * the return value of {@link #getLifecycleState()} accordingly.
   * </p>
   * 
   * @param context
   * @throws LifecycleException
   * @throws InterruptedException
   */
  public void stop(Context context);

  /**
   * <p>
   * Return the current state of the service or component.
   * </p>
   */
  public LifecycleState getLifecycleState();

}
